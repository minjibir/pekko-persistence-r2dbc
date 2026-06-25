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

import scala.concurrent.duration._

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import org.apache.pekko
import pekko.Done
import pekko.actor.testkit.typed.scaladsl.LogCapturing
import pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import pekko.actor.typed.ActorSystem
import pekko.persistence.AtomicWrite
import pekko.persistence.Persistence
import pekko.persistence.PersistentRepr
import pekko.persistence.r2dbc.JournalSettings
import pekko.persistence.r2dbc.TestConfig
import pekko.persistence.r2dbc.TestData
import pekko.persistence.r2dbc.TestDbLifecycle
import pekko.persistence.r2dbc.TestActors.Persister
import pekko.persistence.r2dbc.internal.InstantFactory
import pekko.persistence.typed.PersistenceId
import pekko.serialization.SerializationExtension
import pekko.stream.Materializer
import pekko.stream.scaladsl.Sink
import pekko.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.AnyWordSpecLike

object StreamBatchedWriteJournalBatchSpec {
  val config: Config = ConfigFactory
    .parseString("""
      pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.batched-journal"
      pekko.actor.allow-java-serialization = on
      pekko.actor.warn-about-java-serializer-usage = off
      """)
    .withFallback(TestConfig.config)
    .resolve()
}

class StreamBatchedWriteJournalBatchSpec
    extends ScalaTestWithActorTestKit(StreamBatchedWriteJournalBatchSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with ScalaFutures
    with LogCapturing {

  override def typedSystem: ActorSystem[?] = system

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = 10.seconds, interval = 100.millis)

  private implicit val mat: Materializer = Materializer(system)
  private val serialization = SerializationExtension(system)
  private val persistence = Persistence(system)

  private val batchedJournalConfig = system.settings.config.getConfig("pekko.persistence.r2dbc.batched-journal")
  private val batchedJournalSettings = new JournalSettings(batchedJournalConfig)

  private def newDao(): JournalDao =
    new JournalDao(batchedJournalSettings, connectionFactoryProvider)(system.executionContext, system)

  private def serializeEvents(pid: String, seqNrs: Seq[Long]): Seq[JournalDao.SerializedJournalRow] = {
    val atomicWrite = AtomicWrite(seqNrs.map(seqNr =>
      PersistentRepr(payload = s"$pid-$seqNr", sequenceNr = seqNr, persistenceId = pid, writerUuid = "writer")))
    JournalSerialization.serialize(atomicWrite, InstantFactory.now(), serialization, persistence).get
  }

  /** Build a SerializedJournalRow directly so tags, metadata and payload can be controlled for the UNNEST tests. */
  private def rowWith(
      pid: String,
      seqNr: Long,
      tags: Set[String] = Set.empty,
      metadata: Option[JournalDao.SerializedEventMetadata] = None,
      payload: Array[Byte] = null): JournalDao.SerializedJournalRow =
    JournalDao.SerializedJournalRow(
      slice = persistence.sliceForPersistenceId(pid),
      entityType = PersistenceId.extractEntityType(pid),
      persistenceId = pid,
      seqNr = seqNr,
      dbTimestamp = InstantFactory.now(),
      readDbTimestamp = Instant.EPOCH,
      payload = Some(if (payload ne null) payload else s"payload-$seqNr".getBytes(UTF_8)),
      serId = 101,
      serManifest = "manifest",
      writerUuid = "writer",
      tags = tags,
      metadata = metadata)

  private def storedSeqNrs(dao: JournalDao, pid: String): Seq[Long] =
    dao
      .internalCurrentEventsByPersistenceId(pid, 1L, Long.MaxValue)
      .runWith(Sink.seq)
      .futureValue
      .map(_.seqNr)

  private def storedRows(dao: JournalDao, pid: String): Seq[JournalDao.SerializedJournalRow] =
    dao
      .internalCurrentEventsByPersistenceId(pid, 1L, Long.MaxValue)
      .runWith(Sink.seq)
      .futureValue

  "StreamBatchedWriteJournal" should {

    "write events for multiple persistenceIds in a single batch" in {
      val dao = newDao()
      val pidA, pidB, pidC = nextPid()

      val rows = serializeEvents(pidA, Seq(1L, 2L)) ++ serializeEvents(pidB, Seq(1L)) ++
        serializeEvents(pidC, Seq(1L, 2L, 3L))

      dao.writeEventsInBatch(rows).futureValue

      storedSeqNrs(dao, pidA) shouldBe Seq(1L, 2L)
      storedSeqNrs(dao, pidB) shouldBe Seq(1L)
      storedSeqNrs(dao, pidC) shouldBe Seq(1L, 2L, 3L)
    }

    "roll back the whole batch when it contains a duplicate sequence number" in {
      val dao = newDao()
      val pidA, pidB = nextPid()

      dao.writeEventsInBatch(serializeEvents(pidA, Seq(1L))).futureValue

      // pidA seqNr 1 is a duplicate of the row already written above, pidB seqNr 1 is new
      val batchWithDuplicate = serializeEvents(pidA, Seq(1L)) ++ serializeEvents(pidB, Seq(1L))
      dao.writeEventsInBatch(batchWithDuplicate).failed.futureValue

      storedSeqNrs(dao, pidA) shouldBe Seq(1L) // unchanged
      storedSeqNrs(dao, pidB) shouldBe empty // rolled back with the failed batch
    }

    "write events for multiple persistenceIds in a single UNNEST batch, preserving tags, metadata and payloads" in {
      val dao = newDao()
      val pidA, pidB = nextPid()

      val meta =
        JournalDao.SerializedEventMetadata(serId = 42, serManifest = "meta-manifest", payload = "meta".getBytes(UTF_8))
      val rows =
        Seq(
          rowWith(pidA, 1L, tags = Set("t1", "t2")),
          rowWith(pidA, 2L, metadata = Some(meta)),
          rowWith(pidB, 1L, tags = Set("only")))

      dao.writeEventsInBatchUnnest(rows).futureValue

      val storedA = storedRows(dao, pidA)
      storedA.map(_.seqNr) shouldBe Seq(1L, 2L)
      storedA.head.tags shouldBe Set("t1", "t2")
      new String(storedA.head.payload.get, UTF_8) shouldBe "payload-1" // bytea round-tripped through hex text[]
      storedA(1).tags shouldBe empty
      storedA(1).metadata.map(m => (m.serId, m.serManifest, new String(m.payload, UTF_8))) shouldBe
      Some((42, "meta-manifest", "meta"))

      val storedB = storedRows(dao, pidB)
      storedB.map(_.seqNr) shouldBe Seq(1L)
      storedB.head.tags shouldBe Set("only")
    }

    "store identical rows via the UNNEST insert and the add()-based batch insert" in {
      val dao = newDao()
      val pidAdd, pidUnnest = nextPid()
      val meta =
        JournalDao.SerializedEventMetadata(serId = 7, serManifest = "m", payload = "meta-payload".getBytes(UTF_8))

      def eventsFor(pid: String) =
        Seq(rowWith(pid, 1L, tags = Set("x", "y")), rowWith(pid, 2L, metadata = Some(meta)))

      dao.writeEventsInBatch(eventsFor(pidAdd)).futureValue
      dao.writeEventsInBatchUnnest(eventsFor(pidUnnest)).futureValue

      def normalize(rows: Seq[JournalDao.SerializedJournalRow]) =
        rows.map(r =>
          (r.seqNr, r.tags, r.payload.map(_.toSeq), r.metadata.map(m => (m.serId, m.serManifest, m.payload.toSeq))))

      normalize(storedRows(dao, pidUnnest)) shouldBe normalize(storedRows(dao, pidAdd))
    }

    "roll back the whole UNNEST batch when it contains a duplicate sequence number" in {
      val dao = newDao()
      val pidA, pidB = nextPid()

      dao.writeEventsInBatchUnnest(Seq(rowWith(pidA, 1L))).futureValue

      val batchWithDuplicate = Seq(rowWith(pidA, 1L), rowWith(pidB, 1L))
      dao.writeEventsInBatchUnnest(batchWithDuplicate).failed.futureValue

      storedSeqNrs(dao, pidA) shouldBe Seq(1L) // unchanged
      storedSeqNrs(dao, pidB) shouldBe empty // rolled back with the failed batch
    }

    "persist and replay events from many concurrent persistent actors" in {
      val numberOfActors = 30
      val eventsPerActor = 5
      val pids = (1 to numberOfActors).map(_ => nextPid())

      val ackProbe = createTestProbe[Done]()
      pids.foreach { pid =>
        val ref = spawn(Persister(PersistenceId.ofUniqueId(pid)))
        (1 to eventsPerActor).foreach(i => ref ! Persister.PersistWithAck(s"e$i", ackProbe.ref))
        ref ! Persister.Stop(ackProbe.ref)
      }
      // all writes acked plus one Stop ack per actor
      ackProbe.receiveMessages(numberOfActors * (eventsPerActor + 1), 30.seconds)

      // restart each actor and verify it recovers all events in order
      val expectedState = (1 to eventsPerActor).map(i => s"e$i").mkString("|")
      pids.foreach { pid =>
        val ref = spawn(Persister(PersistenceId.ofUniqueId(pid)))
        val stateProbe = createTestProbe[String]()
        ref ! Persister.GetState(stateProbe.ref)
        stateProbe.expectMessage(10.seconds, expectedState)
      }
    }

    "reject invalid batch settings" in {
      def parseSettings(overrides: String): StreamBatchedWriteJournal.BatchWriteSettings =
        new StreamBatchedWriteJournal.BatchWriteSettings(
          ConfigFactory.parseString(overrides).withFallback(batchedJournalConfig))

      noException should be thrownBy parseSettings("")
      an[IllegalArgumentException] should be thrownBy parseSettings("batch.max-requests = 0")
      an[IllegalArgumentException] should be thrownBy parseSettings("batch.parallelism = 0")
      an[IllegalArgumentException] should be thrownBy parseSettings("batch.queue-size = 0")
    }

    "default batch.use-unnest to off and allow enabling it" in {
      def parseSettings(overrides: String): StreamBatchedWriteJournal.BatchWriteSettings =
        new StreamBatchedWriteJournal.BatchWriteSettings(
          ConfigFactory.parseString(overrides).withFallback(batchedJournalConfig))

      parseSettings("").useUnnest shouldBe false
      parseSettings("batch.use-unnest = on").useUnnest shouldBe true
    }

    "default batch.unnest-max-payload-size to 8 KiB and validate it" in {
      def parseSettings(overrides: String): StreamBatchedWriteJournal.BatchWriteSettings =
        new StreamBatchedWriteJournal.BatchWriteSettings(
          ConfigFactory.parseString(overrides).withFallback(batchedJournalConfig))

      parseSettings("").unnestMaxPayloadSize shouldBe 8192L
      parseSettings("batch.unnest-max-payload-size = 16 KiB").unnestMaxPayloadSize shouldBe 16384L
      an[IllegalArgumentException] should be thrownBy parseSettings("batch.unnest-max-payload-size = 0")
    }

    "use the UNNEST insert only for batches whose events all fit the payload cap" in {
      val pid = nextPid()
      val small = rowWith(pid, 1L, payload = new Array[Byte](100))
      val large = rowWith(pid, 2L, payload = new Array[Byte](2000))

      StreamBatchedWriteJournal.rowPayloadSize(small) shouldBe 100
      StreamBatchedWriteJournal.unnestEligible(Seq(small, small), maxPayloadSize = 1024) shouldBe true
      // a single oversized event routes the whole (atomic) batch to the add() insert
      StreamBatchedWriteJournal.unnestEligible(Seq(small, large), maxPayloadSize = 1024) shouldBe false
      StreamBatchedWriteJournal.unnestEligible(Seq(large), maxPayloadSize = 1024) shouldBe false
    }

    "count both event and metadata payloads toward the UNNEST payload cap" in {
      val pid = nextPid()
      val meta = JournalDao.SerializedEventMetadata(serId = 1, serManifest = "m", payload = new Array[Byte](50))
      val row = rowWith(pid, 1L, payload = new Array[Byte](100), metadata = Some(meta))

      StreamBatchedWriteJournal.rowPayloadSize(row) shouldBe 150
      StreamBatchedWriteJournal.unnestEligible(Seq(row), maxPayloadSize = 149) shouldBe false
      StreamBatchedWriteJournal.unnestEligible(Seq(row), maxPayloadSize = 150) shouldBe true
    }

    "require app timestamps, monotonic timestamps and a supported dialect" in {
      def settingsWith(overrides: String): JournalSettings =
        new JournalSettings(ConfigFactory.parseString(overrides).withFallback(batchedJournalConfig))

      noException should be thrownBy StreamBatchedWriteJournal.settingRequirements(batchedJournalSettings)
      an[IllegalArgumentException] should be thrownBy
      StreamBatchedWriteJournal.settingRequirements(settingsWith("use-app-timestamp = off"))
      an[IllegalArgumentException] should be thrownBy
      StreamBatchedWriteJournal.settingRequirements(settingsWith("db-timestamp-monotonic-increasing = off"))
      an[IllegalArgumentException] should be thrownBy
      StreamBatchedWriteJournal.settingRequirements(settingsWith("dialect = mysql"))
    }

    "tolerate completing the batching queue after the stream already completed (postStop shutdown race)" in {
      // The journal uses a system scoped materializer so the batching stream can drain on postStop. During
      // ActorSystem shutdown the materializer may complete the stream (and its BoundedSourceQueue) before postStop
      // runs; completing an already completed queue throws IllegalStateException. completeQuietly encodes the contract
      // that postStop stays quiet in that race instead of throwing and logging a spurious error.
      val queue = Source.queue[Int](8).to(Sink.ignore).run()
      queue.complete() // the stream/materializer completes the queue first
      queue.isCompleted shouldBe true

      // a second raw completion (what postStop used to do) throws ...
      an[IllegalStateException] should be thrownBy queue.complete()
      // ... but the guarded completion postStop now uses tolerates the shutdown race
      noException should be thrownBy StreamBatchedWriteJournal.completeQuietly(queue)
    }
  }
}

object StreamBatchedWriteJournalAdaptiveUnnestSpec {
  val config: Config = ConfigFactory
    .parseString("""
      pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.batched-journal"
      pekko.persistence.r2dbc.batched-journal.batch.use-unnest = on
      pekko.persistence.r2dbc.batched-journal.batch.unnest-max-payload-size = 8 KiB
      pekko.actor.allow-java-serialization = on
      pekko.actor.warn-about-java-serializer-usage = off
      """)
    .withFallback(TestConfig.config)
    .resolve()
}

class StreamBatchedWriteJournalAdaptiveUnnestSpec
    extends ScalaTestWithActorTestKit(StreamBatchedWriteJournalAdaptiveUnnestSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[?] = system

  "StreamBatchedWriteJournal with adaptive UNNEST (use-unnest = on)" should {
    "persist and replay both small (UNNEST) and large (add() fallback) events" in {
      val pid = PersistenceId.ofUniqueId(nextPid())
      val ackProbe = createTestProbe[Done]()
      val ref = spawn(Persister(pid))

      val small = "s"
      val large = "x" * (12 * 1024) // exceeds the 8 KiB cap -> routed to the add() insert
      val events = Seq(small, large, small)
      events.foreach(e => ref ! Persister.PersistWithAck(e, ackProbe.ref))
      ackProbe.receiveMessages(events.size, 15.seconds)
      ref ! Persister.Stop(ackProbe.ref)
      ackProbe.receiveMessage()

      // restart and verify all events recovered in order, whichever insert path each took
      val restarted = spawn(Persister(pid))
      val stateProbe = createTestProbe[String]()
      restarted ! Persister.GetState(stateProbe.ref)
      stateProbe.expectMessage(15.seconds, events.mkString("|"))
    }
  }
}
