package org.thp.cortex.services

import org.apache.pekko.actor.ActorSystem
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import play.api.Configuration
import play.api.test.PlaySpecification

import scala.concurrent.duration._
import scala.concurrent.Await

@RunWith(classOf[JUnitRunner])
class AdaptiveConcurrencyLimiterSpec extends PlaySpecification {

  "AdaptiveConcurrencyLimiter.RateLimitSignal" should {

    "detect an HTTP 429 response" in {
      AdaptiveConcurrencyLimiter.RateLimitSignal.detect("HTTP 429: Too Many Requests") must beSome
    }

    "detect a rate limit phrase without an HTTP status" in {
      AdaptiveConcurrencyLimiter.RateLimitSignal.detect("provider rate limit exceeded, slow down") must beSome
    }

    "detect a bare 429 status" in {
      AdaptiveConcurrencyLimiter.RateLimitSignal.detect("429") must beSome
    }

    "extract Retry-After from the analyzer error" in {
      val signal = AdaptiveConcurrencyLimiter.RateLimitSignal.detect("429 Too Many Requests\nRetry-After: 12").get
      signal.retryAfter must beSome(12.seconds)
    }

    "ignore unrelated status codes" in {
      AdaptiveConcurrencyLimiter.RateLimitSignal.detect("scan finished, 503 files were analyzed") must beNone
    }
  }

  "AdaptiveConcurrencyLimiter" should {

    "halve the per-key limit on throttling and keep other keys independent" in {
      implicit val system: ActorSystem = ActorSystem("adaptive-limiter-test")
      try {
        val configuration = Configuration(
          "analyzer.adaptiveConcurrency.max"                  -> 10,
          "analyzer.adaptiveConcurrency.min"                  -> 1,
          "analyzer.adaptiveConcurrency.maxPerAnalyzer"       -> 10,
          "analyzer.adaptiveConcurrency.initial"              -> 8,
          "analyzer.adaptiveConcurrency.successesPerIncrease" -> 2
        )
        val limiter = new AdaptiveConcurrencyLimiter(configuration, system)

        limiter.currentLimit("analyzer-a") must_== 8
        limiter.currentLimit("analyzer-b") must_== 8

        val permit = Await.result(limiter.acquire("analyzer-a"), 1.second)
        permit.release(AdaptiveConcurrencyLimiter.Throttled(None)) must beSome

        limiter.currentLimit("analyzer-a") must_== 4
        limiter.currentLimit("analyzer-b") must_== 8
      } finally Await.result(system.terminate(), 5.seconds)
    }

    "increase the per-key limit after consecutive successes" in {
      implicit val system: ActorSystem = ActorSystem("adaptive-limiter-success-test")
      try {
        val configuration = Configuration(
          "analyzer.adaptiveConcurrency.max"                  -> 10,
          "analyzer.adaptiveConcurrency.min"                  -> 1,
          "analyzer.adaptiveConcurrency.maxPerAnalyzer"       -> 10,
          "analyzer.adaptiveConcurrency.initial"              -> 2,
          "analyzer.adaptiveConcurrency.successesPerIncrease" -> 2
        )
        val limiter = new AdaptiveConcurrencyLimiter(configuration, system)

        def succeedOnce(): Unit = {
          val permit = Await.result(limiter.acquire("analyzer-a"), 1.second)
          permit.release(AdaptiveConcurrencyLimiter.Succeeded)
          ()
        }

        succeedOnce()
        limiter.currentLimit("analyzer-a") must_== 2
        succeedOnce()
        limiter.currentLimit("analyzer-a") must_== 3
      } finally Await.result(system.terminate(), 5.seconds)
    }

    "not let a paused key block another key" in {
      implicit val system: ActorSystem = ActorSystem("adaptive-limiter-fairness-test")
      try {
        val configuration = Configuration(
          "analyzer.adaptiveConcurrency.max"                  -> 2,
          "analyzer.adaptiveConcurrency.min"                  -> 1,
          "analyzer.adaptiveConcurrency.maxPerAnalyzer"       -> 1,
          "analyzer.adaptiveConcurrency.initial"              -> 1,
          "analyzer.adaptiveConcurrency.successesPerIncrease" -> 1
        )
        val limiter = new AdaptiveConcurrencyLimiter(configuration, system)

        val a       = Await.result(limiter.acquire("analyzer-a"), 1.second)
        val queuedA = limiter.acquire("analyzer-a")
        queuedA.isCompleted must beFalse

        a.release(AdaptiveConcurrencyLimiter.Throttled(Some(2.seconds)))

        val b = Await.result(limiter.acquire("analyzer-b"), 1.second)
        b.release(AdaptiveConcurrencyLimiter.Succeeded)
        queuedA.isCompleted must beFalse
      } finally Await.result(system.terminate(), 5.seconds)
    }
  }
}
