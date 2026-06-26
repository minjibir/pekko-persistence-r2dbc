/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.r2dbc.journal

import java.time.Instant

import scala.concurrent.duration._

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
import pekko.persistence.typed.PersistenceId
import pekko.serialization.SerializationExtension
import pekko.stream.Materializer
import pekko.stream.scaladsl.Sink
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

  override def typedSystem: ActorSystem[_] = system

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
    JournalSerialization.serialize(atomicWrite, Instant.now(), serialization, persistence).get
  }

  private def storedSeqNrs(dao: JournalDao, pid: String): Seq[Long] =
    dao
      .internalEventsByPersistenceId(pid, 1L, Long.MaxValue)
      .runWith(Sink.seq)
      .futureValue
      .map(_.seqNr)

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
  }
}
