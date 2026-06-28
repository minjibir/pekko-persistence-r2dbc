/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.pekko.persistence.r2dbc.journal

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import org.apache.pekko
import pekko.actor.typed.ActorSystem
import pekko.annotation.InternalApi
import pekko.dispatch.ExecutionContexts
import pekko.persistence.Persistence
import pekko.persistence.r2dbc.ConnectionFactoryProvider
import pekko.persistence.r2dbc.Dialect
import pekko.persistence.r2dbc.JournalSettings
import pekko.persistence.r2dbc.internal.BySliceQuery
import pekko.persistence.r2dbc.internal.EventsByPersistenceIdDao
import pekko.persistence.r2dbc.internal.HighestSequenceNrDao
import pekko.persistence.r2dbc.internal.R2dbcExecutor
import pekko.persistence.r2dbc.internal.Sql.DialectInterpolation
import pekko.persistence.r2dbc.journal.mysql.MySQLJournalDao
import pekko.persistence.typed.PersistenceId
import com.typesafe.config.Config
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
      case null => None
      case metaPayload =>
        Some(
          SerializedEventMetadata(
            serId = row.get[Integer]("meta_ser_id", classOf[Integer]),
            serManifest = row.get("meta_ser_manifest", classOf[String]),
            metaPayload))
    }
  }

  def fromConfig(
      settings: JournalSettings,
      config: Config
  )(implicit system: ActorSystem[_], ec: ExecutionContext): JournalDao = {
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
    implicit val ec: ExecutionContext, system: ActorSystem[_]) extends EventsByPersistenceIdDao
    with HighestSequenceNrDao {
  import JournalDao.SerializedJournalRow
  import JournalDao.log

  implicit protected val dialect: Dialect = settings.dialect
  protected lazy val timestampSql: String = "transaction_timestamp()"
  protected lazy val statementTimestampSql: String = "statement_timestamp()"

  private val persistenceExt = Persistence(system)

  protected val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log, settings.logDbCallsExceeding)(ec, system)

  protected val journalTable: String = settings.journalTableWithSchema

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

  private val deleteEventsSql = sql"""
    DELETE FROM $journalTable
    WHERE persistence_id = ? AND seq_nr <= ?"""
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
        .bind(8, write.payload.get)

      if (write.tags.isEmpty)
        stmt.bindNull(9, classOf[Array[String]])
      else
        stmt.bind(9, write.tags.toArray)

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
        result.map(_ => events.head.dbTimestamp)(ExecutionContexts.parasitic)
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
        result.map(_.head)(ExecutionContexts.parasitic)
      } else {
        result.map(_ => events.head.dbTimestamp)(ExecutionContexts.parasitic)
      }
    }
  }

  /**
   * Write events that may belong to *different* persistenceIds in a single statement (one `add()` batch in one
   * transaction). Used by [[StreamBatchedWriteJournal]] to coalesce concurrent writes from many entities into a single
   * database round-trip.
   *
   * In contrast to [[writeEvents]] this requires application timestamps (`use-app-timestamp = on`) and monotonically
   * increasing timestamps (`db-timestamp-monotonic-increasing = on`) so that each row carries its own `db_timestamp`
   * and no per-persistenceId timestamp subselect is needed. Those requirements are enforced when the
   * [[StreamBatchedWriteJournal]] starts. No timestamp is returned because the rows can belong to different
   * persistenceIds.
   */
  def writeEventsInBatch(events: Seq[SerializedJournalRow]): Future[Unit] = {
    require(events.nonEmpty)

    val MAX_CHUNK_SIZE = 4000
    val chunks = events.grouped(MAX_CHUNK_SIZE).toList

    // 1. Safely split the existing single-row SQL provided by the Dialect
    val sqlParts = insertEventBatchSql.split("(?i)\\sVALUES\\s")
    val baseSql = sqlParts.head.trim
    val singleRowValues = sqlParts.last.trim // e.g., "($1, ... $14)" or "(?, ... ?)"

    // 2. Auto-detect if the dialect uses indexed markers (Postgres/Yugabyte) or generic (MySQL)
    val usesIndexedMarkers = singleRowValues.contains("$")

    val chunkFutures = chunks.map { chunk =>
      // 3. Generate the correct placeholder string for the dialect
      val placeholders = if (usesIndexedMarkers) {
        // Postgres / Yugabyte: ($1, ..., $14), ($15, ..., $28)
        chunk.zipWithIndex.map { case (_, i) =>
          val offset = i * 14
          s"($$${offset + 1}, $$${offset + 2}, $$${offset + 3}, $$${offset + 4}, $$${offset + 5}, $$${offset + 6}, $$${offset + 7}, $$${offset + 8}, $$${offset + 9}, $$${offset + 10}, $$${offset + 11}, $$${offset + 12}, $$${offset + 13}, $$${offset + 14})"
        }.mkString(", ")
      } else {
        // MySQL: (?, ..., ?), (?, ..., ?)
        chunk.map { _ =>
          "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        }.mkString(", ")
      }

      val multiRowSql = s"$baseSql VALUES $placeholders"

      val result = r2dbcExecutor.updateInBatch(s"multi-row insert [${chunk.size}] events") { connection =>
        val stmt = connection.createStatement(multiRowSql)

        // 4. The binding logic is 0-indexed and exactly the same for ALL databases!
        chunk.zipWithIndex.foreach { case (write, idx) =>
          val offset = idx * 14

          stmt.bind(offset + 0, write.slice)
          stmt.bind(offset + 1, write.entityType)
          stmt.bind(offset + 2, write.persistenceId)
          stmt.bind(offset + 3, write.seqNr)
          stmt.bind(offset + 4, write.writerUuid)
          stmt.bind(offset + 5, "") // FIXME event adapter
          stmt.bind(offset + 6, write.serId)
          stmt.bind(offset + 7, write.serManifest)
          stmt.bind(offset + 8, write.payload.get)

          if (write.tags.isEmpty)
            stmt.bindNull(offset + 9, classOf[Array[String]])
          else
            stmt.bind(offset + 9, write.tags.toArray)

          write.metadata match {
            case Some(m) =>
              stmt.bind(offset + 10, m.serId)
              stmt.bind(offset + 11, m.serManifest)
              stmt.bind(offset + 12, m.payload)
            case None =>
              stmt.bindNull(offset + 10, classOf[Integer])
              stmt.bindNull(offset + 11, classOf[String])
              stmt.bindNull(offset + 12, classOf[Array[Byte]])
          }

          stmt.bind(offset + 13, write.dbTimestamp)
        }

        stmt
      }

      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote chunk of [{}] events using multi-row insert", chunk.size)
        }(ExecutionContexts.parasitic)

      result
    }

    Future.sequence(chunkFutures).map(_ => ())(ExecutionContexts.parasitic)
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
          .bind(8, Array.emptyByteArray)
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

      result.map(_ => ())(ExecutionContexts.parasitic)
    }
  }

}
