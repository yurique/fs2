/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2
package concurrent

import cats.implicits._
import cats.effect.IO
import scala.concurrent.duration._

// TODO
//@munit.IgnoreSuite
class TopicSuite extends Fs2Suite {
  test("subscribers see all elements published") {
    Topic[IO, Int](-1).flatMap { topic =>
      val count = 100
      val subs = 10
      val publisher = Stream.sleep[IO](1.second) ++ Stream
        .range(0, count)
        .covary[IO]
        .through(topic.publish)
      val subscriber =
        topic.subscribe(Int.MaxValue).take(count + 1.toLong).fold(Vector.empty[Int]) {
          _ :+ _
        }

      Stream
        .range(0, subs)
        .map(idx => subscriber.map(idx -> _))
        .append(publisher.drain)
        .parJoin(subs + 1)
        .compile
        .toVector
        .map { result =>
          val expected = (for { i <- 0 until subs } yield i).map { idx =>
            idx -> (for { i <- -1 until count } yield i).toVector
          }.toMap
          assert(result.toMap.size == subs)
          assert(result.toMap == expected)
        }
    }
  }

  test("synchronous publish".flaky) {
    // TODO I think there's a race condition on the signal in this test
    Topic[IO, Int](-1).flatMap { topic =>
      SignallingRef[IO, Int](0).flatMap { signal =>
        val count = 100
        val subs = 10

        val publisher = Stream.sleep[IO](1.second) ++ Stream
          .range(0, count)
          .covary[IO]
          .flatMap(i => Stream.eval(signal.set(i)).map(_ => i))
          .through(topic.publish)
        val subscriber = topic
          .subscribe(1)
          .take(count + 1L)
          .flatMap(is => Stream.eval(signal.get).map(is -> _))
          .fold(Vector.empty[(Int, Int)])(_ :+ _)

        Stream
          .range(0, subs)
          .map(idx => subscriber.map(idx -> _))
          .append(publisher.drain)
          .parJoin(subs + 1)
          .compile
          .toVector
          .map { result =>
            assert(result.toMap.size == subs)

            result.foreach {
              case (_, subResults) =>
                val diff: Set[Int] = subResults.map {
                  case (read, state) => Math.abs(state - read)
                }.toSet
                assert(diff.min == 0 || diff.min == 1)
                assert(diff.max == 0 || diff.max == 1)
            }
          }
      }
    }
  }

  test("discrete-size-signal-is-discrete") {
    def p =
      Stream
        .eval(Topic[IO, Int](0))
        .flatMap { topic =>
          def subscribers = topic.subscribers
          def subscribe =
            Stream.fixedDelay[IO](200.millis).evalMap { _ =>
              topic.subscribe(10).compile.drain.start.void
            }
          subscribers.concurrently(subscribe)
        }
        .interruptWhen(Stream.sleep[IO](2.seconds).as(true))

    p.compile.toList
      .map(it =>
        assert(it.size <= 11)
      ) // if the stream won't be discrete we will get much more size notifications
  }

  test("unregister subscribers under concurrent load".ignore) {
    Topic[IO, Int](0)
      .flatMap { topic =>
        Stream
          .range(0, 500)
          .map(_ => topic.subscribe(1).interruptWhen(Stream(true)))
          .parJoinUnbounded
          .compile
          .drain >> topic.subscribers.take(1).compile.lastOrError
      }
      .map(it => assert(it == 0))
  }
}
