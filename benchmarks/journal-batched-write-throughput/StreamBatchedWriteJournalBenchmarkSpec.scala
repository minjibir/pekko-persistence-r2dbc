package org.apache.pekko.persistence.r2dbc.journal

// BENCHMARK TEMPLATE - not part of the production build.
//
// To run it, copy this file into core/src/test/scala/org/apache/pekko/persistence/r2dbc/journal/ and run the sbt
// command documented in the sibling README.md, then remove it again so it does not become part of the compiled test
// suite. Once it is in the source tree, `sbt headerCreateAll` adds the license header.
//
// It compares write throughput of three journal strategies across an actor-count sweep so you can see, for a given
// workload, when to use no batching, the add()-based batch, or the UNNEST-based batch:
//   - BenchmarkDefaultManyActorsSpec        -> "default" : the default R2dbcJournal (no batching)
//   - BenchmarkBatchedAddManyActorsSpec     -> "add"     : StreamBatchedWriteJournal, batch.use-unnest = off
//   - BenchmarkBatchedUnnestManyActorsSpec  -> "unnest"  : StreamBatchedWriteJournal, batch.use-unnest = on
// Run all three in one `testOnly` for directly comparable BENCH lines (same session/warmup). Parameters come from
// -Dpekko.bench.* system properties (forwarded to the forked test JVM because they start with "pekko."):
//   -Dpekko.bench.actorCounts=1,10,50,200,500   actor-count sweep (comma separated)
//   -Dpekko.bench.eventsCount=20                 events persisted per actor per iteration
//   -Dpekko.bench.iterations=5                   measured iterations (best is reported)
//   -Dpekko.bench.warmup=2                       warm-up iterations (not measured)
//   -Dpekko.bench.window=10ms                    batched journal batch.window (batched specs only)
//   -Dpekko.bench.maxRequests=100                batched journal batch.max-requests (batched specs only)
//   -Dpekko.bench.unnestMaxPayloadSize=8 KiB     batch.unnest-max-payload-size ("unnest" spec; "1 GiB" = raw uncapped)
//   -Dpekko.bench.payloadSize=0                  event payload size in bytes (0 = tiny default BenchActor)

import scala.concurrent.duration._

import org.apache.pekko
import pekko.actor.ActorRef
import pekko.actor.ActorSystem
import pekko.actor.Props
import pekko.actor.typed.{ ActorSystem => TypedActorSystem }
import pekko.actor.typed.scaladsl.adapter._
import pekko.persistence.PersistentActor
import pekko.persistence.journal.JournalPerfSpec.BenchActor
import pekko.persistence.journal.JournalPerfSpec.Cmd
import pekko.persistence.r2dbc.TestDbLifecycle
import pekko.testkit.TestKit
import pekko.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

/**
 * Persists a fixed-size byte-array payload per command so the benchmark can measure how event size affects throughput
 * (relevant to the UNNEST insert, which hex-encodes bytea payloads). Mirrors JournalPerfSpec.BenchActor's ack protocol:
 * replies with `eventsCount` once that many events are persisted.
 */
class PayloadBenchActor(pid: String, replyTo: ActorRef, eventsCount: Int, payloadSize: Int) extends PersistentActor {
  private var counter = 0
  private val payload: Array[Byte] = Array.fill[Byte](payloadSize)('a'.toByte)

  override def persistenceId: String = pid

  override def receiveCommand: Receive = { case Cmd(_, _) =>
    persist(payload) { _ =>
      counter += 1
      if (counter == eventsCount) replyTo ! eventsCount
    }
  }

  override def receiveRecover: Receive = { case _ => }
}

abstract class BenchmarkManyActorsBase(cfg: Config)
    extends TestKit(ActorSystem("Benchmark", cfg))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with TestDbLifecycle {

  def journalName: String

  protected def windowLabel: String = ""

  override def typedSystem: TypedActorSystem[?] = system.toTyped

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

  private val eventsCount = sys.props.get("pekko.bench.eventsCount").map(_.toInt).getOrElse(20)
  private val iterations = sys.props.get("pekko.bench.iterations").map(_.toInt).getOrElse(5)
  private val warmup = sys.props.get("pekko.bench.warmup").map(_.toInt).getOrElse(2)
  private val payloadSize = sys.props.get("pekko.bench.payloadSize").map(_.toInt).getOrElse(0)
  private val actorCounts: Seq[Int] =
    sys.props
      .get("pekko.bench.actorCounts")
      .map(_.split(",").toSeq.map(_.trim.toInt))
      .getOrElse(Seq(1, 10, 50, 200, 500))

  private def newActor(persistenceId: String, probe: TestProbe): ActorRef =
    if (payloadSize > 0)
      system.actorOf(Props(classOf[PayloadBenchActor], persistenceId, probe.ref, eventsCount, payloadSize))
    else
      system.actorOf(Props(classOf[BenchActor], persistenceId, probe.ref, eventsCount))

  private def payloadLabel: String = if (payloadSize > 0) f" payload=${payloadSize}%-6dB" else ""

  s"[$journalName] write throughput" should {
    "be measured across the actor-count sweep" in {
      actorCounts.foreach { actorCount =>
        var bestMs = Double.MaxValue
        (1 to (warmup + iterations)).foreach { iter =>
          val probe = TestProbe()
          val actors = (1 to actorCount).map(n => newActor(s"$journalName-$actorCount-$iter-$n", probe))
          val startNanos = System.nanoTime()
          for (event <- 1 to eventsCount; actor <- actors) actor ! Cmd("p", event)
          for (_ <- actors) probe.expectMsg(300.seconds, eventsCount)
          val elapsedMs = (System.nanoTime() - startNanos) / 1e6
          actors.foreach(system.stop)
          if (iter > warmup) bestMs = math.min(bestMs, elapsedMs)
        }
        val totalEvents = actorCount.toLong * eventsCount
        val evPerSec = totalEvents * 1000.0 / bestMs
        println(
          f"BENCH[$journalName%-7s]$windowLabel$payloadLabel actors=$actorCount%-4d events/actor=$eventsCount%-3d  best=$bestMs%8.1f ms  $evPerSec%10.0f ev/s")
      }
    }
  }
}

class BenchmarkDefaultManyActorsSpec extends BenchmarkManyActorsBase(R2dbcJournalPerfSpec.config) {
  override def journalName: String = "default"
}

object BenchmarkBatchedManyActorsSpec {
  def window: String = sys.props.getOrElse("pekko.bench.window", "10ms")
  def maxRequests: String = sys.props.getOrElse("pekko.bench.maxRequests", "100")
  // Per-event cap for the UNNEST path. Default matches reference.conf (8 KiB) so the "unnest" spec is adaptive:
  // it uses UNNEST for events within the cap and falls back to add() for larger events. Set a huge value
  // (e.g. "1 GiB") to force raw uncapped UNNEST (e.g. to reproduce the large-payload OOM study).
  def unnestMaxPayloadSize: String = sys.props.getOrElse("pekko.bench.unnestMaxPayloadSize", "8 KiB")

  def config(useUnnest: Boolean): Config =
    ConfigFactory
      .parseString(s"""
        pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.batched-journal"
        pekko.persistence.r2dbc.batched-journal.batch.window = $window
        pekko.persistence.r2dbc.batched-journal.batch.max-requests = $maxRequests
        pekko.persistence.r2dbc.batched-journal.batch.use-unnest = $useUnnest
        pekko.persistence.r2dbc.batched-journal.batch.unnest-max-payload-size = "$unnestMaxPayloadSize"
        """)
      .withFallback(R2dbcJournalPerfSpec.config)

  def label: String = f" window=$window%-4s maxReq=$maxRequests%-5s"
}

// Batched journal with the add()-based multi-row insert (batch.use-unnest = off).
class BenchmarkBatchedAddManyActorsSpec
    extends BenchmarkManyActorsBase(BenchmarkBatchedManyActorsSpec.config(useUnnest = false)) {
  override def journalName: String = "add"
  override protected def windowLabel: String = BenchmarkBatchedManyActorsSpec.label
}

// Batched journal with the UNNEST insert (batch.use-unnest = on), adaptive via unnest-max-payload-size.
class BenchmarkBatchedUnnestManyActorsSpec
    extends BenchmarkManyActorsBase(BenchmarkBatchedManyActorsSpec.config(useUnnest = true)) {
  override def journalName: String = "unnest"
  override protected def windowLabel: String = BenchmarkBatchedManyActorsSpec.label
}
