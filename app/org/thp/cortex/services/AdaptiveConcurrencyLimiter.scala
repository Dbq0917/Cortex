package org.thp.cortex.services

import org.apache.pekko.actor.ActorSystem
import play.api.{Configuration, Logger}

import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.{Inject, Singleton}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

/**
  * Limits the number of concurrent jobs per key (one key per analyzer instance).
  *
  * The limit follows an AIMD (additive increase / multiplicative decrease)
  * policy driven by job results:
  *
  *  - successful jobs slowly increase the limit;
  *  - a rate-limited job halves the limit and pauses only that key. Other keys
  *    keep running and can use the global capacity that has just been freed.
  *
  * The global limit is work-conserving: capacity is not statically divided
  * between analyzers, so a throttled analyzer never reserves slots that another
  * analyzer could use.
  */
@Singleton
class AdaptiveConcurrencyLimiter @Inject() (configuration: Configuration, system: ActorSystem) {

  import AdaptiveConcurrencyLimiter._

  private lazy val logger: Logger = Logger(getClass)

  private val section  = configuration.getOptional[Configuration]("analyzer.adaptiveConcurrency")
  val enabled: Boolean = section.flatMap(_.getOptional[Boolean]("enabled")).getOrElse(true)

  private val fixedPoolSize: Int =
    configuration.getOptional[Int]("analyzer.thread-pool-executor.fixed-pool-size").getOrElse(20)

  private val globalLimit: Int =
    section.flatMap(_.getOptional[Int]("max")).getOrElse(fixedPoolSize).max(1)
  private val minimumPerKey: Int =
    section.flatMap(_.getOptional[Int]("min")).getOrElse(1).max(1).min(globalLimit)
  private val maximumPerKey: Int =
    section.flatMap(_.getOptional[Int]("maxPerAnalyzer")).getOrElse(globalLimit).max(minimumPerKey).min(globalLimit)
  private val initialPerKey: Int =
    section
      .flatMap(_.getOptional[Int]("initial"))
      .getOrElse(math.min(4, maximumPerKey))
      .max(minimumPerKey)
      .min(maximumPerKey)
  private val successesPerIncrease: Int =
    section.flatMap(_.getOptional[Int]("successesPerIncrease")).getOrElse(2).max(1)
  private val backoffBase: FiniteDuration =
    section.flatMap(_.getOptional[FiniteDuration]("backoff")).getOrElse(1.second)
  private val backoffMax: FiniteDuration =
    section.flatMap(_.getOptional[FiniteDuration]("maxBackoff")).getOrElse(30.seconds)
  private val retryAfterCap: FiniteDuration =
    section.flatMap(_.getOptional[FiniteDuration]("retryAfterCap")).getOrElse(5.minutes)
  val maxAttempts: Int =
    section.flatMap(_.getOptional[Int]("maxRetries")).getOrElse(3).max(0) + 1
  val retryBackoff: FiniteDuration = backoffBase

  private val lock                                   = new Object
  private val keyStates                              = mutable.Map.empty[String, KeyState]
  private var globalInFlight                         = 0
  private val waiters                                = mutable.ArrayBuffer.empty[Waiter]
  implicit private val schedulerEc: ExecutionContext = system.dispatcher
  private def nowNanos: Long                         = System.nanoTime()
  private def stateFor(key: String): KeyState        = keyStates.getOrElseUpdate(key, new KeyState(initialPerKey, backoffBase))
  private def canRun(state: KeyState): Boolean       = state.inFlight < state.limit && nowNanos >= state.pausedUntilNanos
  private def canGrant(state: KeyState): Boolean     = globalInFlight < globalLimit && canRun(state)
  private def grant(state: KeyState, key: String): Permit = {
    state.inFlight += 1
    globalInFlight += 1
    new Permit(key)
  }

  def currentLimit(key: String): Int = lock.synchronized(stateFor(key).limit)
  def totalInFlight: Int             = lock.synchronized(globalInFlight)
  def isPaused(key: String): Boolean = lock.synchronized(nowNanos < stateFor(key).pausedUntilNanos)

  /**
    * Acquires one execution slot for `key`. The returned future is completed
    * when both the per-key and the global concurrency limits allow the job to
    * run. Waiters are served in FIFO order among keys that are currently
    * runnable, so a paused key cannot block another key.
    */
  def acquire(key: String): Future[Permit] =
    lock.synchronized {
      val state = stateFor(key)
      if (canGrant(state)) Future.successful(grant(state, key))
      else {
        val promise = Promise[Permit]()
        waiters += new Waiter(key, promise)
        if (state.pausedUntilNanos > nowNanos)
          scheduleWakeup(Duration.fromNanos(state.pausedUntilNanos - nowNanos))
        promise.future
      }
    }

  private[services] def release(permit: Permit, outcome: Outcome): Option[FiniteDuration] = {
    val delay = lock.synchronized {
      val state = stateFor(permit.key)
      state.inFlight = math.max(0, state.inFlight - 1)
      globalInFlight = math.max(0, globalInFlight - 1)

      outcome match {
        case Succeeded =>
          state.backoff = backoffBase
          state.successes += 1
          if (state.successes >= successesPerIncrease && state.limit < maximumPerKey) {
            state.limit += 1
            state.successes = 0
            logger.info(s"Adaptive concurrency for ${permit.key} increased to ${state.limit} after success")
          } else if (state.limit >= maximumPerKey) {
            state.successes = 0
          }
          None

        case Failed =>
          state.successes = 0
          None

        case Throttled(retryAfter) =>
          val previousLimit = state.limit
          state.limit = decreaseLimit(state.limit, minimumPerKey)
          state.successes = 0
          val sleep = retryDelay(state.backoff, backoffMax, retryAfter, retryAfterCap)
          state.pausedUntilNanos = nowNanos + sleep.toNanos
          state.backoff = nextBackoff(state.backoff, backoffMax)
          logger.warn(
            s"Adaptive concurrency for ${permit.key} reduced from $previousLimit to ${state.limit}; " +
              s"pausing this analyzer for ${sleep.toMillis} ms"
          )
          Some(sleep)
      }
    }
    drain()
    delay
  }

  private def drain(): Unit = {
    val granted                               = mutable.ArrayBuffer.empty[(Waiter, Permit)]
    var earliestPause: Option[FiniteDuration] = None

    lock.synchronized {
      var continue = true
      while (continue && globalInFlight < globalLimit) {
        val index = waiters.indexWhere(waiter => canGrant(stateFor(waiter.key)))
        if (index >= 0) {
          val waiter = waiters.remove(index)
          granted += waiter -> grant(stateFor(waiter.key), waiter.key)
        } else continue = false
      }

      if (waiters.nonEmpty && globalInFlight < globalLimit) {
        earliestPause = waiters
          .map(waiter => stateFor(waiter.key))
          .filter(state => nowNanos < state.pausedUntilNanos)
          .map(state => Duration.fromNanos(state.pausedUntilNanos - nowNanos))
          .reduceOption(_ min _)
          .filter(_ > Duration.Zero)
      }
    }

    granted.foreach {
      case (waiter, permit) =>
        waiter.promise.trySuccess(permit)
        ()
    }
    earliestPause.foreach(scheduleWakeup)
  }

  private def scheduleWakeup(delay: FiniteDuration): Unit =
    if (delay > Duration.Zero)
      system.scheduler.scheduleOnce(delay)(drain())

  final class Permit private[AdaptiveConcurrencyLimiter] (private[services] val key: String) {
    private val released = new AtomicBoolean(false)

    def release(outcome: Outcome): Option[FiniteDuration] =
      if (released.compareAndSet(false, true)) AdaptiveConcurrencyLimiter.this.release(this, outcome)
      else None
  }

  final private class KeyState(var limit: Int, var backoff: FiniteDuration) {
    var inFlight: Int          = 0
    var successes: Int         = 0
    var pausedUntilNanos: Long = 0L
  }

  final private class Waiter(val key: String, val promise: Promise[Permit])
}

object AdaptiveConcurrencyLimiter {

  sealed trait Outcome
  case object Succeeded                                          extends Outcome
  case object Failed                                             extends Outcome
  final case class Throttled(retryAfter: Option[FiniteDuration]) extends Outcome

  final case class RateLimitSignal(retryAfter: Option[FiniteDuration])

  object RateLimitSignal {
    private val RetryAfterPattern = """(?i)retry[-_ ]?after(?:\s*[:=]\s*|\s+)(\d+(?:\.\d+)?)""".r
    private val RateLimitWords = Seq(
      "rate limit",
      "rate-limit",
      "ratelimit",
      "too many requests",
      "quota exceeded",
      "quota exhausted",
      "throttl"
    )
    private val HttpStatusWords = Seq("http", "status", "error", "request", "response", "retry")
    private val Status429       = """\b429\b""".r
    private val Status503       = """\b503\b""".r

    /**
      * Analyzer failures are delivered as free-form text, so an explicit status
      * code is accepted only when the message looks like an HTTP/API error.
      * Retry-After is parsed when present so the pause follows the upstream
      * instruction instead of an arbitrary exponential delay.
      */
    def detect(message: String): Option[RateLimitSignal] = {
      val lower = message.toLowerCase
      val retryAfter = RetryAfterPattern
        .findFirstMatchIn(message)
        .map(m => m.group(1).toDouble.seconds)

      val hasRateLimitWord = RateLimitWords.exists(lower.contains)
      val hasStatusWord    = HttpStatusWords.exists(lower.contains)
      val hasStatus429     = Status429.findFirstIn(lower).isDefined
      val hasStatus503     = Status503.findFirstIn(lower).isDefined && (lower.contains("unavailable") || hasStatusWord)

      if (hasRateLimitWord || retryAfter.isDefined || hasStatus429 || (hasStatusWord && hasStatus503))
        Some(RateLimitSignal(retryAfter))
      else None
    }
  }

  private[services] def decreaseLimit(current: Int, minimum: Int): Int =
    math.max(minimum, math.max(1, current / 2))

  private[services] def nextBackoff(current: FiniteDuration, maximum: FiniteDuration): FiniteDuration = {
    val doubled = current * 2
    if (doubled > maximum) maximum else doubled
  }

  private[services] def retryDelay(
      backoff: FiniteDuration,
      backoffMax: FiniteDuration,
      retryAfter: Option[FiniteDuration],
      retryAfterCap: FiniteDuration,
      random: Random = Random
  ): FiniteDuration = {
    val cappedBackoff = if (backoff > backoffMax) backoffMax else backoff
    val jittered      = (cappedBackoff.toMillis * (0.8 + random.nextDouble() * 0.4)).toLong.millis
    val instructed = retryAfter
      .map(delay => if (delay > retryAfterCap) retryAfterCap else delay)
      .getOrElse(Duration.Zero)
    if (jittered > instructed) jittered else instructed
  }
}
