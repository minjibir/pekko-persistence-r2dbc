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

import scala.concurrent.{ ExecutionContext, Future }

import org.apache.pekko
import pekko.actor.typed.ActorSystem
import pekko.annotation.InternalApi
import pekko.persistence.Persistence
import pekko.persistence.r2dbc.ConnectionFactoryProvider
import pekko.persistence.r2dbc.Dialect
import pekko.persistence.r2dbc.JournalSettings
import pekko.persistence.r2dbc.internal.BySliceQuery
import pekko.persistence.r2dbc.internal.EventsByPersistenceIdDao
import pekko.persistence.r2dbc.internal.HighestSequenceNrDao
import pekko.persistence.r2dbc.internal.PayloadCodec
import pekko.persistence.r2dbc.internal.PayloadCodec.RichStatement
import pekko.persistence.r2dbc.internal.R2dbcExecutor
import pekko.persistence.r2dbc.internal.Sql.DialectInterpolation
import pekko.persistence.r2dbc.journal.mysql.MySQLJournalDao
import pekko.persistence.typed.PersistenceId
import com.typesafe.config.Config
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object JournalDao {
  val log: Logger = LoggerFactory.getLogger(classOf[JournalDao])
  val EmptyDbTimestamp: Instant = Instant.EPOCH

  final case class SerializedJournalRow(
      slice: Int,
      entityType: String,
      persistenceId: String,
      seqNr: Long,
      dbTimestamp: Instant,
      readDbTimestamp: Instant,
      payload: Option[Array[Byte]],
      serId: Int,
      serManifest: String,
      writerUuid: String,
      tags: Set[String],
      metadata: Option[SerializedEventMetadata])
      extends BySliceQuery.SerializedRow

  final case class SerializedEventMetadata(serId: Int, serManifest: String, payload: Array[Byte])

  def readMetadata(row: Row): Option[SerializedEventMetadata] = {
    row.get("meta_payload", classOf[Array[Byte]]) match {
      case null        => None
      case metaPayload =>
        Some(
          SerializedEventMetadata(
            serId = row.get[Integer]("meta_ser_id", classOf[Integer]),
            serManifest = row.get("meta_ser_manifest", classOf[String]),
            metaPayload))
    }
  }

  /**
   * Lowercase hex encoding of a `bytea` payload. Used by the UNNEST-based batch insert to pass binary payloads through
   * a `text[]` array parameter (the r2dbc-postgresql array codec does not support `bytea[]`), decoded back to `bytea`
   * in SQL with `decode(?, 'hex')`.
   */
  private[journal] def toHex(bytes: Array[Byte]): String = {
    val hex = new Array[Char](bytes.length * 2)
    var i = 0
    while (i < bytes.length) {
      val b = bytes(i) & 0xFF
      hex(i * 2) = Character.forDigit(b >>> 4, 16)
      hex(i * 2 + 1) = Character.forDigit(b & 0x0F, 16)
      i += 1
    }
    new String(hex)
  }

  /**
   * Encode a set of tags as a JSON array string, e.g. `["a","b"]`. Used by the UNNEST-based batch insert because a
   * per-row `text[]` column cannot be expressed as a (ragged) multi-dimensional array parameter; the JSON `text[]` is
   * rebuilt into a `text[]` per row in SQL with `jsonb_array_elements_text`.
   */
  private[journal] def tagsToJsonArray(tags: Set[String]): String = {
    val sb = new java.lang.StringBuilder()
    sb.append('[')
    var first = true
    tags.foreach { tag =>
      if (!first) sb.append(',')
      first = false
      appendJsonString(sb, tag)
    }
    sb.append(']')
    sb.toString
  }

  private def appendJsonString(sb: java.lang.StringBuilder, s: String): Unit = {
    sb.append('"')
    var i = 0
    while (i < s.length) {
      s.charAt(i) match {
        case '"'           => sb.append("\\\"")
        case '\\'          => sb.append("\\\\")
        case '\n'          => sb.append("\\n")
        case '\r'          => sb.append("\\r")
        case '\t'          => sb.append("\\t")
        case '\b'          => sb.append("\\b")
        case '\f'          => sb.append("\\f")
        case c if c < 0x20 => sb.append("\\u%04x".format(c.toInt))
        case c             => sb.append(c)
      }
      i += 1
    }
    sb.append('"')
  }

  def fromConfig(
      settings: JournalSettings,
      config: Config
  )(implicit system: ActorSystem[?], ec: ExecutionContext): JournalDao = {
    val connectionFactory =
      ConnectionFactoryProvider(system).connectionFactoryFor(settings.useConnectionFactory, config)
    settings.dialect match {
      case Dialect.Postgres | Dialect.Yugabyte =>
        new JournalDao(settings, connectionFactory)
      case Dialect.MySQL =>
        new MySQLJournalDao(settings, connectionFactory)
    }
  }
}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class JournalDao(val settings: JournalSettings, connectionFactory: ConnectionFactory)(
    implicit val ec: ExecutionContext, system: ActorSystem[?]) extends EventsByPersistenceIdDao
    with HighestSequenceNrDao {
  import JournalDao.SerializedJournalRow
  import JournalDao.log

  implicit protected val dialect: Dialect = settings.dialect
  protected lazy val timestampSql: String = "transaction_timestamp()"
  protected lazy val statementTimestampSql: String = "statement_timestamp()"

  private val persistenceExt = Persistence(system)

  protected val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, settings.logDbCallsExceeding)(ec, system)

  protected val journalTable: String = settings.journalTableWithSchema
  protected implicit val journalPayloadCodec: PayloadCodec = settings.journalPayloadCodec

  protected def bindTagsForWrite(stmt: Statement, tags: Set[String], index: Int): Statement =
    if (tags.isEmpty) stmt.bindNull(index, classOf[Array[String]])
    else stmt.bind(index, tags.toArray)

  protected val (insertEventWithParameterTimestampSql: String, insertEventWithTransactionTimestampSql: String) = {
    val baseSql =
      s"INSERT INTO $journalTable " +
      "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "

    // The subselect of the db_timestamp of previous seqNr for same pid is to ensure that db_timestamp is
    // always increasing for a pid (time not going backwards).
    // TODO we could skip the subselect when inserting seqNr 1 as a possible optimization
    def timestampSubSelect =
      s"(SELECT db_timestamp + '1 microsecond'::interval FROM $journalTable " +
      "WHERE persistence_id = ? AND seq_nr = ?)"

    val insertEventWithParameterTimestampSql = {
      if (settings.dbTimestampMonotonicIncreasing)
        sql"$baseSql ?) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(?, $timestampSubSelect)) RETURNING db_timestamp"
    }

    val insertEventWithTransactionTimestampSql = {
      if (settings.dbTimestampMonotonicIncreasing)
        sql"$baseSql transaction_timestamp()) RETURNING db_timestamp"
      else
        sql"$baseSql GREATEST(transaction_timestamp(), $timestampSubSelect)) RETURNING db_timestamp"
    }

    (insertEventWithParameterTimestampSql, insertEventWithTransactionTimestampSql)
  }

  /**
   * Insert statement used by [[writeEventsInBatch]] to write rows for possibly different persistenceIds in one `add()`
   * batch. The `db_timestamp` is always supplied as a parameter (application timestamp) and there is no per-row
   * timestamp subselect, so there is nothing to return.
   */
  protected val insertEventBatchSql: String = sql"""
    INSERT INTO $journalTable
    (slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

  /**
   * Prototype alternative to [[insertEventBatchSql]] that inserts the whole coalesced batch with a single
   * `UNNEST`-based multi-row statement (one execution instead of one prepared-statement execution per row). Selected
   * by [[StreamBatchedWriteJournal]] when `batch.use-unnest = on`; only valid for the postgres and yugabyte dialects
   * with `bytea` payloads.
   *
   * Each column is supplied as a parallel array parameter. `bytea` payloads are passed as lowercase hex `text[]` and
   * decoded in SQL (the r2dbc-postgresql array codec does not support `bytea[]`). The per-row `tags text[]` - which
   * cannot be expressed as a ragged multi-dimensional array parameter - is passed as a JSON `text[]` and rebuilt with
   * `jsonb_array_elements_text`. `lazy` so it is only interpolated when the postgres/yugabyte UNNEST path is used.
   */
  protected lazy val insertEventBatchUnnestSql: String = sql"""
    INSERT INTO $journalTable
    (slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp)
    SELECT
      d.slice, d.entity_type, d.persistence_id, d.seq_nr, d.writer, d.adapter_manifest, d.event_ser_id, d.event_ser_manifest,
      decode(d.event_payload_hex, 'hex'),
      CASE WHEN d.tags_json IS NULL THEN NULL ELSE ARRAY(SELECT jsonb_array_elements_text(d.tags_json::jsonb)) END,
      d.meta_ser_id, d.meta_ser_manifest,
      CASE WHEN d.meta_payload_hex IS NULL THEN NULL ELSE decode(d.meta_payload_hex, 'hex') END,
      d.db_timestamp::timestamptz
    FROM unnest(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      AS d(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload_hex, tags_json, meta_ser_id, meta_ser_manifest, meta_payload_hex, db_timestamp)"""

  private val deleteEventsSql = sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = ? AND seq_nr <= ?"""

  private val deleteEventsFromToSql = sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?"""

  private val selectLowestSequenceNrSql = sql"""
    SELECT MIN(seq_nr) from $journalTable WHERE persistence_id = ?"""

  private val insertDeleteMarkerSql = sql"""
    INSERT INTO $journalTable
    (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
    VALUES (?, ?, ?, ?, $timestampSql, ?, ?, ?, ?, ?, ?)"""

  /**
   * All events must be for the same persistenceId.
   *
   * The returned timestamp should be the `db_timestamp` column and it is used in published events when that feature is
   * enabled.
   *
   * Note for implementing future database dialects: If a database dialect can't efficiently return the timestamp column
   * it can return `JournalDao.EmptyDbTimestamp` when the pub-sub feature is disabled. When enabled it would have to use
   * a select (in same transaction).
   */
  def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant] = {
    require(events.nonEmpty)

    // it's always the same persistenceId for all events
    val persistenceId = events.head.persistenceId
    val previousSeqNr = events.head.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = events.head.dbTimestamp == Instant.EPOCH

    def bind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind(0, write.slice)
        .bind(1, write.entityType)
        .bind(2, write.persistenceId)
        .bind(3, write.seqNr)
        .bind(4, write.writerUuid)
        .bind(5, "") // FIXME event adapter
        .bind(6, write.serId)
        .bind(7, write.serManifest)
        .bindPayload(8, write.payload.get)

      bindTagsForWrite(stmt, write.tags, 9)

      // optional metadata
      write.metadata match {
        case Some(m) =>
          stmt
            .bind(10, m.serId)
            .bind(11, m.serManifest)
            .bind(12, m.payload)
        case None =>
          stmt
            .bindNull(10, classOf[Integer])
            .bindNull(11, classOf[String])
            .bindNull(12, classOf[Array[Byte]])
      }

      if (useTimestampFromDb) {
        if (!settings.dbTimestampMonotonicIncreasing)
          stmt
            .bind(13, write.persistenceId)
            .bind(14, previousSeqNr)
      } else {
        if (settings.dbTimestampMonotonicIncreasing)
          stmt
            .bind(13, write.dbTimestamp)
        else
          stmt
            .bind(13, write.dbTimestamp)
            .bind(14, write.persistenceId)
            .bind(15, previousSeqNr)
      }

      stmt
    }

    val insertSql =
      if (useTimestampFromDb) insertEventWithTransactionTimestampSql
      else insertEventWithParameterTimestampSql

    val totalEvents = events.size
    if (totalEvents == 1) {
      val result = r2dbcExecutor.updateOneReturning(s"insert [$persistenceId]")(
        connection => bind(connection.createStatement(insertSql), events.head),
        row => row.get(0, classOf[Instant]))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
        }
      if (useTimestampFromDb) {
        result
      } else {
        result.map(_ => events.head.dbTimestamp)(ExecutionContext.parasitic)
      }
    } else {
      val result = r2dbcExecutor.updateInBatchReturning(s"batch insert [$persistenceId], [$totalEvents] events")(
        connection =>
          events.zipWithIndex.foldLeft(connection.createStatement(insertSql)) { case (stmt, (write, idx)) =>
            if (idx != 0) {
              stmt.add()
            }
            bind(stmt, write)
          },
        row => row.get(0, classOf[Instant]))
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
        }
      if (useTimestampFromDb) {
        result.map(_.head)(ExecutionContext.parasitic)
      } else {
        result.map(_ => events.head.dbTimestamp)(ExecutionContext.parasitic)
      }
    }
  }

  /**
   * Write events that may belong to *different* persistenceIds in a single statement (one `add()` batch in one
   * transaction). This is used by [[StreamBatchedWriteJournal]] to coalesce concurrent writes from many entities into
   * a single database round-trip.
   *
   * In contrast to [[writeEvents]] this requires application timestamps (`use-app-timestamp = on`) and monotonically
   * increasing timestamps (`db-timestamp-monotonic-increasing = on`) so that each row carries its own `db_timestamp`
   * and no per-persistenceId timestamp subselect is needed. Those requirements are enforced when the
   * [[StreamBatchedWriteJournal]] starts. No timestamp is returned because the rows can belong to different
   * persistenceIds.
   */
  def writeEventsInBatch(events: Seq[SerializedJournalRow]): Future[Unit] = {
    require(events.nonEmpty)

    def bind(stmt: Statement, write: SerializedJournalRow): Statement = {
      stmt
        .bind(0, write.slice)
        .bind(1, write.entityType)
        .bind(2, write.persistenceId)
        .bind(3, write.seqNr)
        .bind(4, write.writerUuid)
        .bind(5, "") // FIXME event adapter
        .bind(6, write.serId)
        .bind(7, write.serManifest)
        .bindPayload(8, write.payload.get)

      bindTagsForWrite(stmt, write.tags, 9)

      write.metadata match {
        case Some(m) =>
          stmt
            .bind(10, m.serId)
            .bind(11, m.serManifest)
            .bind(12, m.payload)
        case None =>
          stmt
            .bindNull(10, classOf[Integer])
            .bindNull(11, classOf[String])
            .bindNull(12, classOf[Array[Byte]])
      }

      stmt.bind(13, write.dbTimestamp)
    }

    val result = r2dbcExecutor.updateInBatch(s"batch insert [${events.size}] events")(connection =>
      events.zipWithIndex.foldLeft(connection.createStatement(insertEventBatchSql)) {
        case (stmt, (write, idx)) =>
          if (idx != 0) {
            stmt.add()
          }
          bind(stmt, write)
      })

    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote batch of [{}] events", events.size)
      }

    result.map(_ => ())(ExecutionContext.parasitic)
  }

  /**
   * Prototype `UNNEST`-based variant of [[writeEventsInBatch]]: inserts the whole coalesced batch with a single
   * multi-row statement (see [[insertEventBatchUnnestSql]]) instead of an R2DBC `add()` batch, trading one execution
   * per row for a single execution with column-oriented array parameters. Selected by [[StreamBatchedWriteJournal]]
   * when `batch.use-unnest = on`; only valid for the postgres/yugabyte dialects with `bytea` payloads.
   *
   * Runs in a transaction (via `updateInBatch`) so the whole batch still rolls back atomically, preserving the
   * bisection retry semantics of the [[StreamBatchedWriteJournal]].
   */
  def writeEventsInBatchUnnest(events: Seq[SerializedJournalRow]): Future[Unit] = {
    require(events.nonEmpty)
    val n = events.size

    // column-oriented arrays, one element per row (boxed so nullable columns can carry nulls)
    val slices = new Array[java.lang.Integer](n)
    val entityTypes = new Array[String](n)
    val persistenceIds = new Array[String](n)
    val seqNrs = new Array[java.lang.Long](n)
    val writers = new Array[String](n)
    val adapterManifests = new Array[String](n)
    val eventSerIds = new Array[java.lang.Integer](n)
    val eventSerManifests = new Array[String](n)
    val eventPayloadsHex = new Array[String](n)
    val tagsJson = new Array[String](n)
    val metaSerIds = new Array[java.lang.Integer](n)
    val metaSerManifests = new Array[String](n)
    val metaPayloadsHex = new Array[String](n)
    val dbTimestamps = new Array[String](n)

    var i = 0
    events.foreach { write =>
      slices(i) = Int.box(write.slice)
      entityTypes(i) = write.entityType
      persistenceIds(i) = write.persistenceId
      seqNrs(i) = Long.box(write.seqNr)
      writers(i) = write.writerUuid
      adapterManifests(i) = "" // FIXME event adapter
      eventSerIds(i) = Int.box(write.serId)
      eventSerManifests(i) = write.serManifest
      eventPayloadsHex(i) = JournalDao.toHex(write.payload.get)
      // empty tags are stored as SQL NULL, matching the add()-based insert
      tagsJson(i) = if (write.tags.isEmpty) null else JournalDao.tagsToJsonArray(write.tags)
      write.metadata match {
        case Some(m) =>
          metaSerIds(i) = Int.box(m.serId)
          metaSerManifests(i) = m.serManifest
          metaPayloadsHex(i) = JournalDao.toHex(m.payload)
        case None =>
          metaSerIds(i) = null
          metaSerManifests(i) = null
          metaPayloadsHex(i) = null
      }
      dbTimestamps(i) = write.dbTimestamp.toString
      i += 1
    }

    val result = r2dbcExecutor.updateInBatch(s"batch insert via unnest [$n] events")(connection =>
      connection
        .createStatement(insertEventBatchUnnestSql)
        .bind(0, slices)
        .bind(1, entityTypes)
        .bind(2, persistenceIds)
        .bind(3, seqNrs)
        .bind(4, writers)
        .bind(5, adapterManifests)
        .bind(6, eventSerIds)
        .bind(7, eventSerManifests)
        .bind(8, eventPayloadsHex)
        .bind(9, tagsJson)
        .bind(10, metaSerIds)
        .bind(11, metaSerManifests)
        .bind(12, metaPayloadsHex)
        .bind(13, dbTimestamps))

    if (log.isDebugEnabled())
      result.foreach { _ =>
        log.debug("Wrote batch of [{}] events via unnest", n)
      }

    result.map(_ => ())(ExecutionContext.parasitic)
  }

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    val deleteMarkerSeqNrFut =
      if (toSequenceNr == Long.MaxValue)
        readHighestSequenceNr(persistenceId, 0L)
      else
        Future.successful(toSequenceNr)

    deleteMarkerSeqNrFut.flatMap { deleteMarkerSeqNr =>
      def bindDeleteMarker(stmt: Statement): Statement = {
        stmt
          .bind(0, slice)
          .bind(1, entityType)
          .bind(2, persistenceId)
          .bind(3, deleteMarkerSeqNr)
          .bind(4, "")
          .bind(5, "")
          .bind(6, 0)
          .bind(7, "")
          .bindPayloadOption(8, None)
          .bind(9, true)
      }

      val result = r2dbcExecutor.update(s"delete [$persistenceId]") { connection =>
        Vector(
          connection
            .createStatement(deleteEventsSql)
            .bind(0, persistenceId)
            .bind(1, toSequenceNr),
          bindDeleteMarker(connection.createStatement(insertDeleteMarkerSql)))
      }

      if (log.isDebugEnabled)
        result.foreach(updatedRows =>
          log.debug("Deleted [{}] events for persistenceId [{}]", updatedRows.head, persistenceId))

      result.map(_ => ())(ExecutionContext.parasitic)
    }
  }

  private[r2dbc] def readLowestSequenceNr(persistenceId: String): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select lowest seqNr [$persistenceId]")(
        connection =>
          connection
            .createStatement(selectLowestSequenceNrSql)
            .bind(0, persistenceId),
        row => {
          val seqNr = row.get(0, classOf[java.lang.Long])
          if (seqNr eq null) 0L else seqNr.longValue
        })
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContext.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr =>
        log.debug("Lowest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr: java.lang.Long))

    result
  }

  private def highestSeqNrForDelete(persistenceId: String, toSequenceNr: Long): Future[Long] = {
    if (toSequenceNr == Long.MaxValue) readHighestSequenceNr(persistenceId, 0L)
    else Future.successful(toSequenceNr)
  }

  private def lowestSequenceNrForDelete(persistenceId: String, toSeqNr: Long, batchSize: Int): Future[Long] = {
    if (toSeqNr <= batchSize) {
      Future.successful(1L)
    } else {
      readLowestSequenceNr(persistenceId)
    }
  }

  /**
   * Delete events up to and including `toSequenceNr` for `persistenceId` in batches.
   *
   * If `resetSequenceNumber` is `false` a delete marker is left at the highest deleted sequence number, so that the
   * actor can continue from the next sequence number. This is the typical use case for cleanup of older events.
   *
   * If `resetSequenceNumber` is `true` the sequence number will be reset to 1 when the actor is started again with
   * the same `persistenceId`. WARNING: reusing the same `persistenceId` after resetting the sequence number should
   * be avoided, since it might be confusing to reuse the same sequence number for new events.
   *
   * @param batchSize number of events to delete per batch (use `CleanupSettings.eventsJournalDeleteBatchSize`)
   */
  def deleteEventsTo(
      persistenceId: String,
      toSequenceNr: Long,
      resetSequenceNumber: Boolean,
      batchSize: Int): Future[Unit] = {
    def insertDeleteMarkerStmt(deleteMarkerSeqNr: Long, connection: Connection): Statement = {
      val entityType = PersistenceId.extractEntityType(persistenceId)
      val slice = persistenceExt.sliceForPersistenceId(persistenceId)
      connection
        .createStatement(insertDeleteMarkerSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, deleteMarkerSeqNr)
        .bind(4, "")
        .bind(5, "")
        .bind(6, 0)
        .bind(7, "")
        .bind(8, Array.emptyByteArray)
        .bind(9, true)
    }

    def deleteBatch(from: Long, to: Long, lastBatch: Boolean): Future[Unit] = {
      (if (lastBatch && !resetSequenceNumber) {
         r2dbcExecutor
           .update(s"delete [$persistenceId] and insert marker") { connection =>
             Vector(
               connection
                 .createStatement(deleteEventsFromToSql)
                 .bind(0, persistenceId)
                 .bind(1, from)
                 .bind(2, to),
               insertDeleteMarkerStmt(to, connection))
           }
           .map(_.head)
       } else {
         r2dbcExecutor
           .updateOne(s"delete [$persistenceId]") { connection =>
             connection
               .createStatement(deleteEventsFromToSql)
               .bind(0, persistenceId)
               .bind(1, from)
               .bind(2, to)
           }
       }).map(deletedRows =>
        if (log.isDebugEnabled) {
          log.debug(
            "Deleted [{}] events for persistenceId [{}], from seq num [{}] to [{}]",
            deletedRows: java.lang.Long,
            persistenceId,
            from: java.lang.Long,
            to: java.lang.Long)
        })(ExecutionContext.parasitic)
    }

    def deleteInBatches(from: Long, maxTo: Long): Future[Unit] = {
      if (from + batchSize > maxTo) {
        deleteBatch(from, maxTo, lastBatch = true)
      } else {
        val to = from + batchSize - 1
        deleteBatch(from, to, lastBatch = false).flatMap(_ => deleteInBatches(to + 1, maxTo))
      }
    }

    for {
      toSeqNr <- highestSeqNrForDelete(persistenceId, toSequenceNr)
      fromSeqNr <- lowestSequenceNrForDelete(persistenceId, toSeqNr, batchSize)
      _ <- deleteInBatches(fromSeqNr, toSeqNr)
    } yield ()
  }

}
