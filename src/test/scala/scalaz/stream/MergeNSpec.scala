package scalaz.stream

import org.scalacheck.Prop._
import org.scalacheck.Properties
import scalaz.concurrent.{Strategy, Task}
import scalaz.stream.Process._
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.SyncVar
import scalaz.\/


object MergeNSpec extends Properties("mergeN") {

  implicit val S = Strategy.DefaultStrategy
  implicit val scheduler = scalaz.stream.DefaultScheduler

  property("basic") = forAll {
    (l: List[Int]) =>

      val count = (l.size % 6) max 1

      val ps =
        emitAll(for (i <- 0 until count) yield {
          emitAll(l.filter(v => (v % count).abs == i)).toSource
        }).toSource

      val result =
        merge.mergeN(ps).runLog.timed(3000).run

      (result.sorted.toList == l.sorted.toList) :| "All elements were collected"

  }


  property("source-cleanup-down-done") = secure {
    val cleanups = new AtomicInteger(0)
    val srcCleanup = new AtomicInteger(0)

    val ps =
      emitAll(for (i <- 0 until 10) yield {
        (Process.constant(i+100) onComplete eval(Task.delay{Thread.sleep(100); cleanups.incrementAndGet()}))
      }).toSource onComplete eval_(Task.delay(srcCleanup.set(99)))



    //this makes sure we see at least one value from sources
    // and therefore we won`t terminate downstream to early.
    merge.mergeN(ps).scan(Set[Int]())({
      case (sum, next) => sum + next
    }).takeWhile(_.size < 10).runLog.timed(3000).run

    (cleanups.get == 10) :| s"Cleanups were called on upstreams: ${cleanups.get}" &&
      (srcCleanup.get == 99) :| "Cleanup on source was called"
  }

  // unlike source-cleanup-down-done it focuses on situations where upstreams are in async state,
  // and thus will block until interrupted.
  property("source-cleanup-async-down-done") = secure {
    val cleanups = new AtomicInteger(0)
    val srcCleanup = new AtomicInteger(0)
    //this below is due the non-thread-safety of scala object, we must memoize this here
    val delayEach10 =  Process.awakeEvery(10 seconds)

    def oneUp(index:Int) = (emit(index).toSource ++ delayEach10.map(_=>index)) onComplete
      eval(Task.delay{val i = cleanups.incrementAndGet();Thread.sleep(100);i})

    val ps =
      (emitAll(for (i <- 0 until 10) yield oneUp(i)).toSource ++ delayEach10.drain) onComplete
        eval_(Task.delay(srcCleanup.set(99)))


    merge.mergeN(ps).takeWhile(_ < 9).runLog.timed(3000).run

    (cleanups.get == 10) :| s"Cleanups were called on upstreams: ${cleanups.get}" &&
      (srcCleanup.get == 99) :| "Cleanup on source was called"
  }

  //merges 10k of streams, each with 100 of elements
  property("merge-million") = secure {
    val count = 10000
    val eachSize = 100

    val ps =
      emitAll(for (i <- 0 until count) yield {
        Process.range(0,eachSize)
      }).toSource

    val result = merge.mergeN(ps).fold(0)(_ + _).runLast.timed(120000).run

    (result == Some(49500000)) :| "All items were emitted"
  }

  property("merge-maxOpen") = secure {
    val count = 100
    val eachSize = 10

    val sizeSig = async.signal[Int]

    def incrementOpen =
      sizeSig.compareAndSet({
        case Some(running) => Some(running + 1)
        case None => Some(1)
      })

    def decrementDone =
      sizeSig.compareAndSet({
        case Some(running) => Some(running - 1)
        case None => Some(0)
      })

    val sleep5 = sleep(5 millis)

    val ps =
      emitAll(for (i <- 0 until count) yield {
        eval_(incrementOpen) fby
          Process.range(0,eachSize).flatMap(i=> emit(i) fby sleep5) onComplete
          eval_(decrementDone)
      }).toSource

    val running = new SyncVar[Throwable \/ IndexedSeq[Int]]
    Task.fork(sizeSig.discrete.runLog).runAsync(running.put)

    merge.mergeN(25)(ps).run.timed(10000).run
    sizeSig.close.run

    "mergeN and signal finished" |: running.get(3000).isDefined &&
      ("max 25 were run in parallel" |: running.get.toList.flatten.filter(_ > 25).isEmpty)

  }


  //tests that mergeN correctly terminates with drained process
  property("drain-halt") = secure {

    val effect = Process.constant(()).drain
    val p = Process(1,2)

    merge.mergeN(Process(effect,p)).take(2)
    .runLog.timed(3000).run.size == 2

  }

}
