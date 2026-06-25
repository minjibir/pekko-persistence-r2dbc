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

import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration.{ DurationLong, FiniteDuration }
import scala.util.{ Failure, Success, Try }

import com.typesafe.config.Config
import org.apache.pekko
import pekko.Done
import pekko.actor.typed.ActorSystem
import pekko.actor.typed.scaladsl.adapter._
import pekko.annotation.InternalApi
import pekko.event.Logging
import pekko.persistence.AtomicWrite
import pekko.persistence.Persistence
import pekko.persistence.PersistentRepr
import pekko.persistence.journal.AsyncWriteJournal
import pekko.persistence.r2dbc.Dialect
import pekko.persistence.r2dbc.JournalSettings
import pekko.persistence.r2dbc.internal.InstantFactory
import pekko.persistence.r2dbc.internal.PayloadCodec
import pekko.persistence.r2dbc.internal.PubSub
import pekko.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import pekko.serialization.Serialization
import pekko.serialization.SerializationExtension
import pekko.stream.BoundedSourceQueue
import pekko.stream.Materializer
import pekko.stream.QueueOfferResult
import pekko.stream.SystemMaterializer
import pekko.stream.scaladsl.Sink
import pekko.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object StreamBatchedWriteJournal {

  /**
   * A single `asyncWriteMessages` call (one persistenceId) waiting to be coalesced into a batched database write.
   * `result` is completed when the events have been durably written (or failed).
   */
  final case class BatchedWriteRequest(events: Seq[SerializedJournalRow], result: Promise[Unit])

  /**
   * Settings for the batching stream, read from the `batch { ... }` section of the journal config.
   */
  final class BatchWriteSettings(config: Config) {

    /** Maximum number of write requests coalesced into a single database statement. */
    val maxRequests: Int = config.getInt("batch.max-requests")

    /** Maximum time to wait while coalescing write requests before flushing a batch. */
    val window: FiniteDuration = config.getDuration("batch.window", MILLISECONDS).millis

    /** Number of batches written concurrently. Effectively bounded by `connection-factory.max-size`. */
    val parallelism: Int = config.getInt("batch.parallelism")

    /** Capacity of the bounded buffer in front of the batching stream. Writes are rejected when it overflows. */
    val queueSize: Int = config.getInt("batch.queue-size")

    /**
     * Prototype (experimental): insert each coalesced batch with a single `UNNEST`-based multi-row statement instead
     * of an R2DBC `add()` batch. Only supported for the postgres/yugabyte dialects with `bytea` payloads; otherwise it
     * falls back to the `add()`-based insert. When enabled it is applied *adaptively* per batch - see
     * [[unnestMaxPayloadSize]].
     */
    val useUnnest: Boolean = config.getBoolean("batch.use-unnest")

    /**
     * Per-event payload-size cap (bytes) for the `UNNEST` insert. When [[useUnnest]] is on, a coalesced batch uses
     * `UNNEST` only if every event's serialized payload (event + metadata) is at most this size; a batch containing a
     * larger event falls back to the `add()` insert. This keeps `UNNEST` to the payload sizes where it is faster and
     * bounds its off-heap memory use (roughly `maxRequests * 2 * this`) so a large event cannot OOM the writer.
     */
    val unnestMaxPayloadSize: Long = config.getBytes("batch.unnest-max-payload-size")

    require(maxRequests > 0, s"batch.max-requests must be > 0, was [$maxRequests]")
    require(parallelism > 0, s"batch.parallelism must be > 0, was [$parallelism]")
    require(queueSize > 0, s"batch.queue-size must be > 0, was [$queueSize]")
    require(
      unnestMaxPayloadSize > 0,
      s"batch.unnest-max-payload-size must be > 0, was [$unnestMaxPayloadSize]")
  }

  /** Estimated serialized payload bytes of a row: event payload plus optional metadata payload. */
  private[r2dbc] def rowPayloadSize(row: SerializedJournalRow): Int =
    row.payload.fold(0)(_.length) + row.metadata.fold(0)(_.payload.length)

  /**
   * Whether a coalesced batch may use the `UNNEST` insert, given the per-event payload cap. Applied per batch so a
   * single oversized event routes the whole (atomic) batch to the `add()` insert.
   */
  private[r2dbc] def unnestEligible(events: Seq[SerializedJournalRow], maxPayloadSize: Long): Boolean =
    events.forall(rowPayloadSize(_) <= maxPayloadSize)

  /**
   * The events in a coalesced batch belong to different persistenceIds, so the per-row timestamp subselect used by the
   * default journal cannot be applied. The batched journal therefore requires app timestamps (so each row carries its
   * own db_timestamp) and monotonic timestamps (so no subselect is needed) - the same requirements MySQL has - and is
   * restricted to the dialects that support the batched insert. These requirements are checked when the journal starts.
   */
  def settingRequirements(journalSettings: JournalSettings): Unit = {
    require(
      journalSettings.useAppTimestamp,
      "use-app-timestamp config must be on for the StreamBatchedWriteJournal")
    require(
      journalSettings.dbTimestampMonotonicIncreasing,
      "db-timestamp-monotonic-increasing config must be on for the StreamBatchedWriteJournal")
    journalSettings.dialect match {
      case Dialect.Postgres | Dialect.Yugabyte => () // supported
      case other                               =>
        throw new IllegalArgumentException(
          s"The StreamBatchedWriteJournal only supports the postgres and yugabyte dialects, but was [$other]")
    }
  }

  /**
   * Complete the batching queue, tolerating the shutdown race in which the stream (and therefore its
   * [[pekko.stream.BoundedSourceQueue]]) has already been completed - for example when the system scoped
   * materializer is shut down before this journal actor's `postStop` runs. Completing an already completed
   * `BoundedSourceQueue` throws [[IllegalStateException]]; that case is benign during shutdown, so it is swallowed to
   * keep `postStop` quiet instead of logging a spurious error.
   */
  private[r2dbc] def completeQuietly(queue: BoundedSourceQueue[?]): Unit =
    try queue.complete()
    catch {
      case _: IllegalStateException => () // already completed by a concurrent stream/materializer shutdown
    }
}

/**
 * INTERNAL API
 *
 * A write journal that coalesces concurrent writes from many persistent actors into a single multi-row database
 * statement, trading a small amount of write latency (bounded by `batch.window`) for higher write throughput.
 *
 * Writes are pushed onto a bounded queue, grouped with [[pekko.stream.scaladsl.FlowOps.groupedWithin]] and written in
 * one transaction per group via [[JournalDao.writeEventsInBatch]]. If a coalesced write fails (for example because one
 * of the persistent actors in the group attempted to write a duplicate sequence number) the group is retried by
 * bisecting it so that only the genuinely failing request fails and the rest still succeed.
 *
 * Because the events in a group belong to different persistenceIds, this journal requires application generated and
 * monotonically increasing timestamps (`use-app-timestamp = on` and `db-timestamp-monotonic-increasing = on`) and is
 * only supported for the Postgres and Yugabyte dialects. These requirements are checked when the journal starts.
 *
 * This is an opt-in alternative to [[R2dbcJournal]]; enable it by pointing `pekko.persistence.journal.plugin` at a
 * config block whose `class` is this class (see `pekko.persistence.r2dbc.batched-journal` in `reference.conf`).
 */
@InternalApi
private[r2dbc] final class StreamBatchedWriteJournal(config: Config, cfgPath: String) extends AsyncWriteJournal {
  import R2dbcJournal.WriteFinished
  import R2dbcJournal.deserializeRow
  import StreamBatchedWriteJournal.BatchWriteSettings
  import StreamBatchedWriteJournal.BatchedWriteRequest

  implicit val system: ActorSystem[?] = context.system.toTyped
  implicit val ec: ExecutionContext = context.dispatcher
  // a system scoped materializer so the stream can drain on postStop rather than being torn down with the actor
  private implicit val mat: Materializer = SystemMaterializer(context.system).materializer

  private val log = Logging(context.system, classOf[StreamBatchedWriteJournal])

  private val persistenceExt = Persistence(system)

  private val serialization: Serialization = SerializationExtension(context.system)
  private val journalSettings = JournalSettings(config)
  private val batchSettings = new BatchWriteSettings(config)

  StreamBatchedWriteJournal.settingRequirements(journalSettings)

  private val journalDao = JournalDao.fromConfig(journalSettings, config)

  // Prototype UNNEST insert only supports bytea payloads; fall back to the add()-batched insert otherwise.
  private val useUnnestEnabled: Boolean = {
    val byteaPayloads = journalSettings.journalPayloadCodec == PayloadCodec.ByteArrayCodec
    if (batchSettings.useUnnest && !byteaPayloads)
      log.warning(
        "batch.use-unnest is enabled but is only supported with bytea payloads; " +
        "falling back to add()-batched inserts")
    batchSettings.useUnnest && byteaPayloads
  }

  private val pubSub: Option[PubSub] =
    if (journalSettings.journalPublishEvents) Some(PubSub(system))
    else None

  // if there are pending writes when an actor restarts we must wait for
  // them to complete before we can read the highest sequence number or we will miss it
  private val writesInProgress = new java.util.HashMap[String, Future[?]]()

  private val queue: BoundedSourceQueue[BatchedWriteRequest] =
    Source
      .queue[BatchedWriteRequest](batchSettings.queueSize)
      .groupedWithin(batchSettings.maxRequests, batchSettings.window)
      .mapAsync(batchSettings.parallelism)(writeBatch)
      .to(Sink.ignore)
      .run()

  override def postStop(): Unit = {
    // During ActorSystem shutdown the system scoped materializer may complete the stream (and its queue) before this
    // actor's postStop runs; completing an already completed queue throws, so guard against that benign shutdown race.
    StreamBatchedWriteJournal.completeQuietly(queue)
    super.postStop()
  }

  override def receivePluginInternal: Receive = { case WriteFinished(pid, f) =>
    writesInProgress.remove(pid, f)
  }

  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    // use-app-timestamp is required, so the timestamp is always generated here
    val timestamp = InstantFactory.now()

    // all messages in one call are for the same persistenceId; the persistAsync case (multiple AtomicWrites) is
    // collapsed into a single AtomicWrite as in R2dbcJournal
    val atomicWrite =
      if (messages.size == 1) messages.head
      else AtomicWrite(messages.flatMap(_.payload))

    val writeResult: Future[Instant] =
      JournalSerialization.serialize(atomicWrite, timestamp, serialization, persistenceExt) match {
        case Success(events) =>
          val promise = Promise[Unit]()
          offer(BatchedWriteRequest(events, promise))
          promise.future.map(_ => timestamp)(ExecutionContext.parasitic)
        case Failure(exc) =>
          Future.failed(exc)
      }

    val persistenceId = messages.head.persistenceId
    val writeAndPublishResult: Future[Done] = publish(messages, writeResult)

    writesInProgress.put(persistenceId, writeAndPublishResult)
    writeAndPublishResult.onComplete { _ =>
      self ! WriteFinished(persistenceId, writeAndPublishResult)
    }
    writeAndPublishResult.map(_ => Nil)(ExecutionContext.parasitic)
  }

  private def offer(request: BatchedWriteRequest): Unit =
    queue.offer(request) match {
      case QueueOfferResult.Enqueued => // waiting for the batch window to flush
      case QueueOfferResult.Dropped  =>
        request.result.tryFailure(
          new RuntimeException(
            s"Journal batching queue is full (batch.queue-size = [${batchSettings.queueSize}]), write rejected"))
      case QueueOfferResult.QueueClosed =>
        request.result.tryFailure(new RuntimeException("Journal batching stream is closed"))
      case QueueOfferResult.Failure(exc) =>
        request.result.tryFailure(exc)
    }

  /**
   * Write one coalesced group as a single batch. On success all requests in the group are completed. On failure the
   * group is retried by bisecting it - each half is retried as a smaller batch, recursively - so that only the
   * genuinely failing request(s) fail while the rest still succeed as part of a sub-batch. This isolates a single bad
   * write (for example a duplicate sequence number) in O(log n) batch writes instead of retrying all n requests one by
   * one. Always completes with [[Done]] so the stream keeps running.
   */
  private def writeBatch(requests: immutable.Seq[BatchedWriteRequest]): Future[Done] =
    writeOrBisect(requests).recover { case exc =>
      // last resort, must not let the stream fail; fail any request that is still pending
      requests.foreach(_.result.tryFailure(exc))
      Done
    }(ExecutionContext.parasitic)

  private def writeOrBisect(requests: immutable.Seq[BatchedWriteRequest]): Future[Done] = {
    val events = requests.iterator.flatMap(_.events.iterator).toVector
    // Adaptive: use UNNEST only when enabled AND every event fits the payload cap, so a large event cannot slow the
    // write or OOM the writer (its off-heap buffers scale with batch size * 2 * payload). Otherwise use the add() batch.
    val useUnnest =
      useUnnestEnabled && {
        val eligible =
          StreamBatchedWriteJournal.unnestEligible(events, batchSettings.unnestMaxPayloadSize)
        if (!eligible && log.isDebugEnabled)
          log.debug(
            "Batch of [{}] events contains an event larger than batch.unnest-max-payload-size [{}], using add() insert",
            events.size,
            batchSettings.unnestMaxPayloadSize)
        eligible
      }
    val writeResult =
      if (useUnnest) journalDao.writeEventsInBatchUnnest(events)
      else journalDao.writeEventsInBatch(events)
    writeResult
      .transformWith {
        case Success(_) =>
          requests.foreach(_.result.trySuccess(()))
          if (log.isDebugEnabled)
            log.debug("Wrote batch of [{}] requests, [{}] events", requests.size, events.size)
          Future.successful(Done)
        case Failure(exc) if requests.size == 1 =>
          // isolated the failing request
          requests.head.result.tryFailure(exc)
          Future.successful(Done)
        case Failure(exc) =>
          if (log.isDebugEnabled)
            log.debug(
              "Batched write of [{}] requests failed, bisecting to isolate the failure: {}",
              requests.size,
              exc.toString)
          val (left, right) = requests.splitAt(requests.size / 2)
          // sequential (left then right) to keep the number of in-use connections bounded during the rare failure path
          writeOrBisect(left).flatMap(_ => writeOrBisect(right))(ExecutionContext.parasitic)
      }(ExecutionContext.parasitic)
  }

  // mirrors R2dbcJournal.publish
  private def publish(messages: immutable.Seq[AtomicWrite], dbTimestamp: Future[Instant]): Future[Done] =
    pubSub match {
      case Some(ps) =>
        dbTimestamp.map { timestamp =>
          messages.iterator
            .flatMap(_.payload.iterator)
            .foreach(pr => ps.publish(pr, timestamp))

          Done
        }

      case None =>
        dbTimestamp.map(_ => Done)(ExecutionContext.parasitic)
    }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    log.debug("asyncDeleteMessagesTo persistenceId [{}], toSequenceNr [{}]", persistenceId, toSequenceNr)
    journalDao.deleteMessagesTo(persistenceId, toSequenceNr)
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
      recoveryCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug("asyncReplayMessages persistenceId [{}], fromSequenceNr [{}]", persistenceId, fromSequenceNr)
    val effectiveToSequenceNr =
      if (max == Long.MaxValue) toSequenceNr
      else math.min(toSequenceNr, fromSequenceNr + max - 1)
    journalDao
      .internalCurrentEventsByPersistenceId(persistenceId, fromSequenceNr, effectiveToSequenceNr)
      .runWith(Sink.foreach { row =>
        val repr = deserializeRow(serialization, row)
        recoveryCallback(repr)
      })
      .map(_ => ())
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug("asyncReadHighestSequenceNr [{}] [{}]", persistenceId, fromSequenceNr)
    val pendingWrite = Option(writesInProgress.get(persistenceId)) match {
      case Some(f) =>
        log.debug("Write in progress for [{}], deferring highest seq nr until write completed", persistenceId)
        // we only want to make write - replay sequential, not fail if previous write failed
        f.recover { case _ => Done }(ExecutionContext.parasitic)
      case None => Future.successful(Done)
    }
    pendingWrite.flatMap(_ => journalDao.readHighestSequenceNr(persistenceId, fromSequenceNr))
  }
}
