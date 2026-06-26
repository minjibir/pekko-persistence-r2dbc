/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

package org.apache.pekko.persistence.r2dbc.journal

import org.apache.pekko
import pekko.actor.typed.ActorSystem
import pekko.actor.typed.scaladsl.adapter._
import pekko.persistence.CapabilityFlag
import pekko.persistence.journal.JournalSpec
import pekko.persistence.r2dbc.TestConfig
import pekko.persistence.r2dbc.TestDbLifecycle
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object StreamBatchedWriteJournalSpec {
  def testConfig(): Config = {
    ConfigFactory
      .parseString(s"""
      pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.batched-journal"
      # short window so the TCK's sequential single-actor writes are not slowed down
      pekko.persistence.r2dbc.batched-journal.batch.window = 5 ms
      # allow java serialization when testing
      pekko.actor.allow-java-serialization = on
      pekko.actor.warn-about-java-serializer-usage = off
      """)
      .withFallback(TestConfig.config)
  }
}

/**
 * Runs the Pekko persistence journal TCK against the [[StreamBatchedWriteJournal]] to verify it honours the same
 * contract as the default [[R2dbcJournal]].
 */
class StreamBatchedWriteJournalSpec
    extends JournalSpec(StreamBatchedWriteJournalSpec.testConfig())
    with TestDbLifecycle {
  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.off()
  override def typedSystem: ActorSystem[_] = system.toTyped
}
