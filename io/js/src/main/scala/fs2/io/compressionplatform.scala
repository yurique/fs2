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

import cats.effect.Async
import cats.syntax.all._
import fs2.compression._
import fs2.internal.jsdeps.node.{nodeStrings, zlibMod}
import fs2.io.internal.SuspendedStream
import fs2.compression.internal.{ChunkInflater, InflatePipe, MakeChunkInflater}
import fs2.internal.jsdeps.node.bufferMod.global.Buffer
import fs2.internal.jsdeps.node.streamMod.{Duplex, Readable}
import fs2.io.internal.ByteChunkOps._

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration
import scala.scalajs.js
import scala.scalajs.js.|

private[fs2] trait compressionplatform {

  private implicit def makeChunkInflaterForAsync[F[_]](implicit
      F: Async[F]
  ): MakeChunkInflater[F] = new MakeChunkInflater[F] {

    def withChunkInflater(
        inflateParams: InflateParams
    )(
        body: ChunkInflater[F] => Pull[F, Byte, Unit]
    ): Pull[F, Byte, Unit] =
      body(chunkInflater(inflateParams))

    private def chunkInflater[F[_]](
        inflateParams: InflateParams
    )(implicit F: Async[F]): ChunkInflater[F] = {
      val options = zlibMod
        .ZlibOptions()
        .setChunkSize(inflateParams.bufferSizeOrMinimum.toDouble)

      val writable = (inflateParams.header match {
        case ZLibParams.Header.GZIP => zlibMod.createInflateRaw(options)
        case ZLibParams.Header.ZLIB => zlibMod.createInflate(options)
      }).asInstanceOf[Duplex]
      val readable = writable.asInstanceOf[Readable]
      val inflate = writable.asInstanceOf[zlibMod.Zlib]

      new ChunkInflater[F] {
        def inflateChunk(
            bytesChunk: Chunk[Byte]
        ): Pull[
          F,
          INothing,
          (Chunk[Byte], Int, Boolean)
        ] = // (inflatedBuffer, inflatedBytes, remainingBytes, finished)
          Pull.eval {
            F.async_[(Chunk[Byte], Int, Boolean)] { cb =>
              println()
              println()
              println(s"got chunk to inflate: ${bytesChunk.size} bytes")
              readable.read() match {
                case null =>
                  println(s"  read before write: null")

                  val writtenBefore = inflate.bytesWritten.toLong
                  val buffer = bytesChunk.toUint8Array

                  def tryRead(finished: Boolean) = {
                    val writtenNow = inflate.bytesWritten.toLong
                    val bytesWriten = writtenNow - writtenBefore
                    println(s"  bytes written: ${bytesWriten}")
                    val bytesRemaining = bytesChunk.size - bytesWriten
                    println(s"  bytes remaining: $bytesRemaining bytes")
                    val out = readable.read() match {
                      case null =>
                        println(s"  read null")
                        (Chunk.empty[Byte], bytesRemaining.toInt, finished)
                      case notNull =>
                        val buffer = notNull.asInstanceOf[Buffer]
                        val chunk = buffer.toChunk
                        println(s"  read buffer: ${chunk.size} bytes")
                        (chunk, bytesRemaining.toInt, finished)
                    }
                    cb(out.asRight[Throwable])
                  }

                  val onError: js.Function1[Any, Unit] = e => println(s"readable.error: ${e}")
                  val onEnd: js.Function1[Any, Unit] = _ => {
                    println(s"!!! readable.end")
                    tryRead(true)
                  }

                  val onReadable: js.Function1[Any, Unit] = _ => {
                    println(s"!!! readable.readable")
                    readable.off("error", onError)
                    readable.off("end", onEnd)
                    tryRead(false)
                  }

                  readable.once("error", onError)
                  readable.once("end", onEnd)
                  readable.once("readable", onReadable)

                  val written = writable.write(
                    buffer,
                    (e: js.UndefOr[js.Error | Null]) => println(s"callback: $e")
                    //                  cb(
                    //                    e.toLeft {
                    //
                    //                    }.leftMap(js.JavaScriptException)
                    //                  )
                  )
                  println(s"written: $written")

                case notNull =>
                  val buffer = notNull.asInstanceOf[Buffer]
                  val chunk = buffer.toChunk
                  println(s"  read buffer before write: ${chunk.size} bytes")
                  cb((chunk, bytesChunk.size, false).asRight)
              }

            }
          }
      }
    }
  }

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
            }).asInstanceOf[fs2.io.Duplex]
          })
          .flatMap { case (deflate, out) =>
            out
              .concurrently(in.through(writeWritable[F](deflate.pure.widen)))
              .onFinalize(
                F.async_[Unit](cb => deflate.asInstanceOf[zlibMod.Zlib].close(() => cb(Right(()))))
              )
          }
      }

      override def inflate(inflateParams: InflateParams): Pipe[F, Byte, Byte] =
        InflatePipe.inflateChunks(inflateParams, none, none, none, trailerSize = 0)

      def gzip(
          fileName: Option[String],
          modificationTime: Option[FiniteDuration],
          comment: Option[String],
          deflateParams: DeflateParams
      )(implicit d: DummyImplicit): Pipe[F, Byte, Byte] =
        gzip.gzip(
          fileName,
          modificationTime,
          comment,
          deflate(deflateParams),
          deflateParams,
          fs2.internal.jsdeps.node.processMod.global.process.platform.toString
        )

      def gunzip(inflateParams: InflateParams): Stream[F, Byte] => Stream[F, GunzipResult[F]] =
        gzip.gunzip(
          InflatePipe.inflateAndTrailer(inflateParams, gzip.gzipTrailerBytes),
          inflateParams
        )

    }
}
