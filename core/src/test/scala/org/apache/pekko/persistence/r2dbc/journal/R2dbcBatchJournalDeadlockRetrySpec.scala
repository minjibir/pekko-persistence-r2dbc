/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.r2dbc.journal

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import io.r2dbc.spi.R2dbcNonTransientResourceException
import io.r2dbc.spi.R2dbcRollbackException
import io.r2dbc.spi.R2dbcTransientException
import io.r2dbc.spi.R2dbcTransientResourceException
import org.apache.pekko
import pekko.Done
import pekko.event.NoLogging
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class R2dbcBatchJournalDeadlockRetrySpec extends AnyWordSpecLike with Matchers {

  private implicit val ec: ExecutionContext = ExecutionContext.parasitic

  "R2dbcBatchJournal.isDeadlock" should {

    "classify a MySQL deadlock as retryable" in {
      R2dbcBatchJournal.isDeadlock(new R2dbcTransientResourceException("deadlock", "40001", 1213)) shouldBe true
    }

    "classify a Postgres deadlock as retryable" in {
      R2dbcBatchJournal.isDeadlock(new R2dbcTransientException("deadlock", "40P01", 0) {}) shouldBe true
    }

    "classify a Postgres serialization failure as retryable" in {
      R2dbcBatchJournal.isDeadlock(new R2dbcRollbackException("serialization failure", "40001", 0)) shouldBe true
    }

    "not classify a missing SQL state as retryable" in {
      R2dbcBatchJournal.isDeadlock(new R2dbcTransientResourceException("closed", null, 0)) shouldBe false
    }

    "not classify a data integrity violation as retryable" in {
      R2dbcBatchJournal
        .isDeadlock(new R2dbcDataIntegrityViolationException("duplicate", "23000", 1062)) shouldBe false
    }

    "not classify a non-transient resource failure as retryable" in {
      R2dbcBatchJournal
        .isDeadlock(new R2dbcNonTransientResourceException("connection refused", "08001", 0)) shouldBe false
    }
  }

  "R2dbcBatchJournal.writeBatch" should {

    "retry the whole batch when a write fails with a deadlock" in {
      val promise = Promise[Done]()
      val request = R2dbcBatchJournal.WriteRequest(Seq.empty, Seq.empty, promise)
      var attempts = 0

      val result = R2dbcBatchJournal.writeBatch(NoLogging, Vector(request), deadlockRetriesLeft = 3) { _ =>
        attempts += 1
        if (attempts == 1) Future.failed(new R2dbcTransientResourceException("deadlock", "40001", 1213))
        else {
          promise.trySuccess(Done)
          Future.unit
        }
      }

      Await.result(result, 3.seconds) shouldBe (())
      Await.result(promise.future, 3.seconds) shouldBe Done
      attempts shouldBe 2
    }

    "fail the batch when the deadlock retries are exhausted" in {
      val promise = Promise[Done]()
      val request = R2dbcBatchJournal.WriteRequest(Seq.empty, Seq.empty, promise)
      val failure = new R2dbcTransientResourceException("deadlock", "40001", 1213)
      var attempts = 0

      val result = R2dbcBatchJournal.writeBatch(NoLogging, Vector(request), deadlockRetriesLeft = 2) { _ =>
        attempts += 1
        Future.failed(failure)
      }

      Await.result(result, 3.seconds) shouldBe (())
      Await.result(promise.future.failed, 3.seconds) shouldBe theSameInstanceAs(failure)
      attempts shouldBe 3
    }

    "not retry the whole batch when the deadlock retries are disabled" in {
      val promise = Promise[Done]()
      val request = R2dbcBatchJournal.WriteRequest(Seq.empty, Seq.empty, promise)
      val failure = new R2dbcTransientResourceException("deadlock", "40001", 1213)
      var attempts = 0

      val result = R2dbcBatchJournal.writeBatch(NoLogging, Vector(request), deadlockRetriesLeft = 0) { _ =>
        attempts += 1
        Future.failed(failure)
      }

      Await.result(result, 3.seconds) shouldBe (())
      Await.result(promise.future.failed, 3.seconds) shouldBe theSameInstanceAs(failure)
      attempts shouldBe 1
    }

    "retry the whole batch when the write fails with a serialization failure" in {
      val promise = Promise[Done]()
      val request = R2dbcBatchJournal.WriteRequest(Seq.empty, Seq.empty, promise)
      val failure = new R2dbcRollbackException("serialization failure", "40001", 0)
      var attempts = 0

      val result = R2dbcBatchJournal.writeBatch(NoLogging, Vector(request), deadlockRetriesLeft = 2) { _ =>
        attempts += 1
        if (attempts == 1) Future.failed(failure)
        else {
          promise.trySuccess(Done)
          Future.unit
        }
      }

      Await.result(result, 3.seconds) shouldBe (())
      Await.result(promise.future, 3.seconds) shouldBe Done
      attempts shouldBe 2
    }
  }
}
