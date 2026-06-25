/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pekko.persistence.r2dbc.journal

import java.util.UUID

import scala.concurrent.duration._

import org.apache.pekko
import pekko.actor.ActorSystem
import pekko.actor.typed.{ ActorSystem => TypedActorSystem }
import pekko.actor.typed.scaladsl.adapter._
import pekko.persistence.AtomicWrite
import pekko.persistence.JournalProtocol._
import pekko.persistence.Persistence
import pekko.persistence.PersistentRepr
import pekko.persistence.r2dbc.TestConfig
import pekko.persistence.r2dbc.TestData
import pekko.persistence.r2dbc.TestDbLifecycle
import pekko.testkit.ImplicitSender
import pekko.testkit.TestKit
import pekko.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object StreamBatchedWriteJournalIsolationSpec {
  // a large batch window so that two writes submitted back-to-back are coalesced into the same batch
  val config = ConfigFactory
    .parseString("""
      pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.batched-journal"
      pekko.persistence.r2dbc.batched-journal.batch.window = 500 ms
      pekko.actor.allow-java-serialization = on
      pekko.actor.warn-about-java-serializer-usage = off
      """)
    .withFallback(TestConfig.config)
}

/**
 * Regression test for the failure isolation guarantee: when a coalesced batch write fails because one of the requests
 * is invalid (here a duplicate sequence number), the other requests in the same batch must still succeed.
 */
class StreamBatchedWriteJournalIsolationSpec
    extends TestKit(
      ActorSystem("StreamBatchedWriteJournalIsolationSpec", StreamBatchedWriteJournalIsolationSpec.config))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TestData
    with TestDbLifecycle {

  override def typedSystem: TypedActorSystem[?] = system.toTyped

  private val journal = Persistence(system).journalFor("pekko.persistence.r2dbc.batched-journal")
  private val writerUuid = UUID.randomUUID().toString

  private def atomicWrite(pid: String, seqNr: Long): AtomicWrite =
    AtomicWrite(
      PersistentRepr(payload = s"$pid-$seqNr", sequenceNr = seqNr, persistenceId = pid, writerUuid = writerUuid))

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  "StreamBatchedWriteJournal" should {
    "isolate a failing write so that other writes in the same batch still succeed" in {
      val pidBad = nextPid()
      val pidGood = nextPid()

      // establish pidBad seqNr 1 so that a second write of seqNr 1 is a duplicate
      val setupProbe = TestProbe()
      journal.tell(WriteMessages(List(atomicWrite(pidBad, 1L)), setupProbe.ref, 1), setupProbe.ref)
      setupProbe.expectMsg(10.seconds, WriteMessagesSuccessful)
      setupProbe.expectMsgType[WriteMessageSuccess]

      // submit the duplicate (pidBad seqNr 1) together with a valid write (pidGood seqNr 1) back-to-back so they are
      // coalesced into one batch; the coalesced insert fails on the duplicate and the journal must bisect the batch to
      // isolate the failure
      val badProbe = TestProbe()
      val goodProbe = TestProbe()
      journal.tell(WriteMessages(List(atomicWrite(pidBad, 1L)), badProbe.ref, 2), badProbe.ref)
      journal.tell(WriteMessages(List(atomicWrite(pidGood, 1L)), goodProbe.ref, 3), goodProbe.ref)

      // the good write succeeds even though it was batched with the failing one
      goodProbe.expectMsg(10.seconds, WriteMessagesSuccessful)
      goodProbe.expectMsgType[WriteMessageSuccess]

      // the duplicate write fails in isolation
      badProbe.expectMsgType[WriteMessagesFailed](10.seconds)
      badProbe.expectMsgType[WriteMessageFailure]
    }

    "isolate one failing write among several good ones in the same coalesced batch" in {
      val pidBad = nextPid()

      // establish pidBad seqNr 1 so that a second write of seqNr 1 is a duplicate
      val setupProbe = TestProbe()
      journal.tell(WriteMessages(List(atomicWrite(pidBad, 1L)), setupProbe.ref, 1), setupProbe.ref)
      setupProbe.expectMsg(10.seconds, WriteMessagesSuccessful)
      setupProbe.expectMsgType[WriteMessageSuccess]

      // coalesce four valid writes with the duplicate in the middle, all back-to-back, into one batch; the batch fails
      // and is bisected recursively so that only the duplicate fails
      val goodPids = (1 to 4).map(_ => nextPid())
      val goodProbes = goodPids.map(_ => TestProbe())
      val badProbe = TestProbe()
      journal.tell(WriteMessages(List(atomicWrite(goodPids(0), 1L)), goodProbes(0).ref, 20), goodProbes(0).ref)
      journal.tell(WriteMessages(List(atomicWrite(goodPids(1), 1L)), goodProbes(1).ref, 21), goodProbes(1).ref)
      journal.tell(WriteMessages(List(atomicWrite(pidBad, 1L)), badProbe.ref, 22), badProbe.ref)
      journal.tell(WriteMessages(List(atomicWrite(goodPids(2), 1L)), goodProbes(2).ref, 23), goodProbes(2).ref)
      journal.tell(WriteMessages(List(atomicWrite(goodPids(3), 1L)), goodProbes(3).ref, 24), goodProbes(3).ref)

      // every good write succeeds despite being batched with the failing one
      goodProbes.foreach { p =>
        p.expectMsg(10.seconds, WriteMessagesSuccessful)
        p.expectMsgType[WriteMessageSuccess]
      }

      // the duplicate fails in isolation
      badProbe.expectMsgType[WriteMessagesFailed](10.seconds)
      badProbe.expectMsgType[WriteMessageFailure]
    }
  }
}
