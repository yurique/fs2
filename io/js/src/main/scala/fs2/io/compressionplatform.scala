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
package io

import cats.effect.kernel.Async
import cats.effect.{Deferred, Resource}
import cats.syntax.all._
import fs2.compression._
import fs2.internal.jsdeps.node.zlibMod
import fs2.io.internal.SuspendedStream

import scala.concurrent.duration.FiniteDuration

private[fs2] trait compressionplatform {

  implicit def fs2ioCompressionForAsync[F[_]](implicit F: Async[F]): Compression[F] =
    new Compression.UnsealedCompression[F] {

      private val gzip = new Gzip[F]

      override def deflate(deflateParams: DeflateParams): Pipe[F, Byte, Byte] = in => {
        val options = zlibMod
          .ZlibOptions()
          .setChunkSize(deflateParams.bufferSizeOrMinimum.toDouble)
          .setLevel(deflateParams.level.juzDeflaterLevel.toDouble)
          .setStrategy(deflateParams.strategy.juzDeflaterStrategy.toDouble)
          .setFlush(deflateParams.flushMode.juzDeflaterFlushMode.toDouble)

        Stream
          .resource(suspendReadableAndRead() {
            (deflateParams.header match {
              case ZLibParams.Header.GZIP => zlibMod.createDeflateRaw(options)
              case ZLibParams.Header.ZLIB => zlibMod.createDeflate(options)
            }).asInstanceOf[Duplex]
          })
          .flatMap { case (deflate, out) =>
            out
              .concurrently(in.through(writeWritable[F](deflate.pure.widen)))
              .onFinalize(
                F.async_[Unit](cb => deflate.asInstanceOf[zlibMod.Zlib].close(() => cb(Right(()))))
              )
          }
      }

      override def inflate(inflateParams: InflateParams): Pipe[F, Byte, Byte] = in =>
        inflateAndTrailer(inflateParams, 0)(in).flatMap(_._1)

      private def inflateAndTrailer(
          inflateParams: InflateParams,
          trailerSize: Int
      ): Stream[F, Byte] => Stream[F, (Stream[F, Byte], Deferred[F, Chunk[Byte]])] = in => {
        val options = zlibMod
          .ZlibOptions()
          .setChunkSize(inflateParams.bufferSizeOrMinimum.toDouble)

        Stream
          .resource {
            suspendReadableAndRead() {
              (inflateParams.header match {
                case ZLibParams.Header.GZIP => zlibMod.createInflateRaw(options)
                case ZLibParams.Header.ZLIB => zlibMod.createInflate(options)
              }).asInstanceOf[Duplex]
            }
          }
          .flatMap { case (inflate, out) =>
            Stream.resource(SuspendedStream(in)).flatMap { suspendedIn =>
              Stream.eval(F.ref(Chunk.empty[Byte])).flatMap { lastChunk =>
                Stream.eval(F.ref(0)).flatMap { bytesPiped =>
                  Stream.eval(F.deferred[Int]).flatMap { bytesWritten =>
                    Stream.eval(F.deferred[Chunk[Byte]]).flatMap { trailerChunk =>
                      val trackedStream =
                        suspendedIn.stream.chunks.evalTap { chunk =>
                          bytesPiped.update(_ + chunk.size) >> lastChunk.set(chunk)
                        }.unchunks
                      val inflated = out
                        .concurrently(
                          trackedStream
                            .through(writeWritable[F](inflate.asInstanceOf[Writable].pure))
                        )
                        .onFinalize {
                          F.delay(inflate.asInstanceOf[zlibMod.Zlib].bytesWritten.toLong.toInt)
                            .flatMap(bytesWritten.complete) >>
                            F.async_[Unit] { cb =>
                              inflate.asInstanceOf[zlibMod.Zlib].close(() => cb(Right(())))
                            }
                        }

                      Stream
                        .resource {
                          F.background {
                            (bytesWritten.get, bytesPiped.get, lastChunk.get).mapN {
                              (bytesWritten, bytesPiped, lastChunk) =>
                                val bytesAvailable = bytesPiped - bytesWritten
                                val headTrailerBytes = lastChunk.takeRight(bytesAvailable)
                                val bytesToPull = trailerSize - headTrailerBytes.size
                                if (bytesToPull > 0) {
                                  suspendedIn.stream
                                    .take(bytesToPull)
                                    .chunkAll
                                    .compile
                                    .lastOrError
                                    .flatMap { remainingBytes =>
                                      trailerChunk.complete(
                                        headTrailerBytes ++ remainingBytes
                                      )
                                    }
                                } else {
                                  trailerChunk.complete(
                                    headTrailerBytes.take(trailerSize)
                                  )
                                }
                            }.flatten
                          }
                        }
                        .as(
                          (inflated, trailerChunk)
                        )
                    }
                  }
                }
              }
            }
          }
      }

      def gzip2(
          fileName: Option[String],
          modificationTime: Option[FiniteDuration],
          comment: Option[String],
          deflateParams: DeflateParams
      ): Pipe[F, Byte, Byte] =
        gzip.gzip(fileName, modificationTime, comment, deflate(deflateParams), deflateParams)

      def gunzip(inflateParams: InflateParams): Stream[F, Byte] => Stream[F, GunzipResult[F]] =
        gzip.gunzip(inflateAndTrailer(inflateParams, gzip.gzipTrailerBytes), inflateParams)

    }
}
