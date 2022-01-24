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

package fs2.compression.internal

import cats.syntax.all._
import cats.effect.{Ref, Sync}
import fs2.compression.InflateParams
import fs2.{Chunk, INothing, Pull, Stream}

object InflatePipe {

  private val emptySlice = Chunk.ArraySlice(Array.empty[Byte], 0, 0)

  def inflateChunks[F[_]](
      inflateParams: InflateParams,
      trailerChunkRef: Option[Ref[F, Chunk[Byte]]],
      bytesWrittenRef: Option[Ref[F, Long]],
      crc32Ref: Option[Ref[F, Long]],
      trailerSize: Int
  )(implicit
      makeChunkInflater: MakeChunkInflater[F],
      F: Sync[F]
  ): Stream[F, Byte] => Stream[F, Byte] =
    stream =>
      makeChunkInflater
        .withChunkInflater(inflateParams) { inflater =>
          val track =
            trailerChunkRef.isDefined && bytesWrittenRef.isDefined && crc32Ref.isDefined
          val crcBuilder = new CrcBuilder

          def setRefs(trailerBytes: Chunk[Byte], bytesWritten: Long) =
            Pull.eval {
              trailerChunkRef.fold(F.unit)(_.set(trailerBytes)) >>
                bytesWrittenRef.fold(F.unit)(_.set(bytesWritten)) >>
                crc32Ref.fold(F.unit)(_.set(crcBuilder.getValue))
            }

          def setTrailerChunk(
              remaining: Chunk[Byte],
              bytesWritten: Long
          ): Stream[F, Byte] => Pull[F, INothing, Unit] =
            _.pull.uncons.flatMap {
              case None =>
                setRefs(remaining, bytesWritten)
              case Some((chunk, rest)) =>
                if (remaining.size + chunk.size > trailerSize) {
                  setRefs(remaining ++ chunk.take(trailerSize - remaining.size), bytesWritten)
                } else {
                  setTrailerChunk(remaining ++ chunk, bytesWritten)(rest)
                }
            }

          def inflateChunk(
              bytesChunk: Chunk.ArraySlice[Byte],
              inflatedBytesSoFar: Long
          ): Pull[F, Byte, (Chunk[Byte], Long, Boolean)] =
            inflater.inflateChunk(bytesChunk).flatMap {
              case (inflatedArray, inflatedLength, remainingBytes, finished) =>
//                println(s"inflatedBytes: ${inflatedLength}")
//                println(s"remainingBytes: ${remainingBytes.length}")
//                println(s"finished: $finished")
                if (track) crcBuilder.update(inflatedArray, 0, inflatedLength)
                val inflatedChunk = copyAsChunkBytes(inflatedArray, inflatedLength)
                Pull.output(inflatedChunk) >> {
                  if (!finished) {
                    if (remainingBytes.nonEmpty) {
                      inflateChunk(
                        remainingBytes,
                        inflatedBytesSoFar + inflatedChunk.size
                      )
                    } else {
                      Pull.pure((Chunk.empty, inflatedBytesSoFar + inflatedChunk.size, false))
                    }
                  } else {
                    Pull.pure(
                      (
                        remainingBytes,
                        inflatedBytesSoFar + inflatedChunk.size,
                        true
                      )
                    )
                  }
                }
            }

          def drain(inflatedBytesSoFar: Long): Pull[F, Byte, (Chunk[Byte], Long, Boolean)] =
//            println(s"[drain]... inflatedBytesSoFar: $inflatedBytesSoFar")

            inflater.inflateChunk(emptySlice).flatMap {
              case (inflatedArray, inflatedLength, remainingBytes, finished) =>
//                println(s"[drain] inflatedBytes: ${inflatedLength}")
//                println(s"[drain] remainingBytes: $remainingBytes")
//                println(s"[drain] finished: $finished")
                val p =
                  if (inflatedLength == 0) {
                    Pull.done
                  } else {
                    if (track) crcBuilder.update(inflatedArray, 0, inflatedLength)
                    val inflatedChunk = copyAsChunkBytes(inflatedArray, inflatedLength)
                    Pull.output(inflatedChunk)
                  }

                p >> {
                  if (!finished) {
                    drain(
                      inflatedBytesSoFar + inflatedLength
                    )
                  } else {
                    Pull.pure(
                      (
                        remainingBytes,
                        inflatedBytesSoFar + inflatedLength,
                        true
                      )
                    )
                  }
                }

            }

          def pull(inflatedBytesSoFar: Long): Stream[F, Byte] => Pull[F, Byte, Unit] = in =>
            in.pull.uncons.flatMap {
              case None =>
                inflater.end.flatMap {
                  case true =>
                    drain(inflatedBytesSoFar).flatMap {
                      case (
                            _,
                            _,
                            false // not finished
                          ) =>
                        Pull.raiseError(new RuntimeException("drain did not "))
                      case (
                            remaining,
                            inflatedBytesTotal,
                            true // finished
                          ) =>
                        if (track)
                          setTrailerChunk(remaining, inflatedBytesTotal)(Stream.empty)
                        else
                          Pull.done
                    }
                  case false =>
                    if (track)
                      setTrailerChunk(Chunk.empty, inflatedBytesSoFar)(Stream.empty)
                    else
                      Pull.done
                }
              case Some((chunk, rest)) =>
                inflateChunk(chunk.toArraySlice, 0).flatMap {
                  case (
                        remaining @ _, // remaining will be Chunk.empty
                        chunkBytesWritten,
                        false // not finished
                      ) =>
                    pull(inflatedBytesSoFar + chunkBytesWritten)(rest)
                  case (
                        remaining,
                        chunkBytesWritten,
                        true // finished
                      ) =>
                    if (track)
                      setTrailerChunk(remaining, inflatedBytesSoFar + chunkBytesWritten)(rest)
                    else
                      Pull.done
                }
            }

          pull(0)(stream)
        }
        .stream

  def inflateAndTrailer[F[_]](
      inflateParams: InflateParams,
      trailerSize: Int
  )(implicit makeChunkInflater: MakeChunkInflater[F], F: Sync[F]): Stream[F, Byte] => Stream[
    F,
    (Stream[F, Byte], Ref[F, Chunk[Byte]], Ref[F, Long], Ref[F, Long])
  ] = in =>
    Stream.suspend {
      Stream
        .eval(
          (
            Ref.of[F, Chunk[Byte]](Chunk.empty),
            Ref.of[F, Long](0),
            Ref.of[F, Long](0)
          ).tupled
        )
        .map { case (trailerChunk, bytesWritten, crc32) =>
          (
            inflateChunks(
              inflateParams,
              trailerChunk.some,
              bytesWritten.some,
              crc32.some,
              trailerSize
            ).apply(
              in
            ),
            trailerChunk,
            bytesWritten,
            crc32
          )
        }
    }

  private def copyAsChunkBytes(values: Array[Byte], length: Int): Chunk[Byte] =
    if (length > 0) {
      val target = new Array[Byte](length)
      System.arraycopy(values, 0, target, 0, length)
      Chunk.array(target, 0, length)
    } else Chunk.empty[Byte]

}
