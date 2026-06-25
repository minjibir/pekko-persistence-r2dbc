/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/*
 * Copyright (C) 2021 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.pekko.persistence.r2dbc.journal

import java.time.Instant

import scala.util.Try

import org.apache.pekko
import pekko.annotation.InternalApi
import pekko.persistence.AtomicWrite
import pekko.persistence.Persistence
import pekko.persistence.journal.Tagged
import pekko.persistence.r2dbc.journal.JournalDao.SerializedEventMetadata
import pekko.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import pekko.persistence.typed.PersistenceId
import pekko.serialization.Serialization
import pekko.serialization.Serializers

/**
 * INTERNAL API
 *
 * Shared serialization of an [[AtomicWrite]] into [[SerializedJournalRow]] rows. Used by both the default
 * [[R2dbcJournal]] and the batching [[StreamBatchedWriteJournal]] so the serialization logic is defined in a single
 * place.
 */
@InternalApi private[r2dbc] object JournalSerialization {

  /**
   * Serialize all events of an `AtomicWrite` to [[SerializedJournalRow]]. The `dbTimestamp` of each row is set to
   * `timestamp`; pass [[JournalDao.EmptyDbTimestamp]] to let the database assign the timestamp.
   *
   * The result is wrapped in a `Try` so that a serialization failure can be turned into a rejected write rather than a
   * crashing journal.
   */
  def serialize(
      atomicWrite: AtomicWrite,
      timestamp: Instant,
      serialization: Serialization,
      persistence: Persistence): Try[Seq[SerializedJournalRow]] = Try {
    atomicWrite.payload.map { pr =>
      val (event, tags) = pr.payload match {
        case Tagged(payload, tags) =>
          (payload.asInstanceOf[AnyRef], tags)
        case other =>
          (other.asInstanceOf[AnyRef], Set.empty[String])
      }

      val entityType = PersistenceId.extractEntityType(pr.persistenceId)
      val slice = persistence.sliceForPersistenceId(pr.persistenceId)

      val serialized = serialization.serialize(event).get
      val serializer = serialization.findSerializerFor(event)
      val manifest = Serializers.manifestFor(serializer, event)
      val id: Int = serializer.identifier

      val metadata = pr.metadata.map { meta =>
        val m = meta.asInstanceOf[AnyRef]
        val serializedMeta = serialization.serialize(m).get
        val metaSerializer = serialization.findSerializerFor(m)
        val metaManifest = Serializers.manifestFor(metaSerializer, m)
        val metaId: Int = metaSerializer.identifier
        SerializedEventMetadata(metaId, metaManifest, serializedMeta)
      }

      SerializedJournalRow(
        slice,
        entityType,
        pr.persistenceId,
        pr.sequenceNr,
        timestamp,
        JournalDao.EmptyDbTimestamp,
        Some(serialized),
        id,
        manifest,
        pr.writerUuid,
        tags,
        metadata)
    }
  }
}
