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
import fs2.internal.jsdeps.node.streamMod.{Duplex, Readable, Writable}
import fs2.io.internal.ByteChunkOps._

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration
import scala.scalajs.js
import scala.scalajs.js.{JavaScriptException, |}

private[fs2] trait compressionplatform {

  private implicit def makeChunkInflaterForAsync[F[_]](implicit
      F: Async[F]
  ): MakeChunkInflater[F] = new MakeChunkInflater[F] {

    def withChunkInflater(
        inflateParams: InflateParams
    )(
        body: ChunkInflater[F] => Pull[F, Byte, Unit]
    ): Pull[F, Byte, Unit] =
      Pull.bracketCase[F, Byte, (Duplex, Readable, zlibMod.Zlib), Unit](
        Pull.pure {
          val options = zlibMod
            .ZlibOptions()
            .setChunkSize(inflateParams.bufferSizeOrMinimum.toDouble)

//          fs2.internal.jsdeps.node.nodeConsoleMod.global.console.log(
//            "options",
//            options
//          )

          val writable = (inflateParams.header match {
            case ZLibParams.Header.GZIP => zlibMod.createInflateRaw(options)
            case ZLibParams.Header.ZLIB => zlibMod.createInflate(options)
          }).asInstanceOf[Duplex]
          val readable = writable.asInstanceOf[Readable]
          val inflate = writable.asInstanceOf[zlibMod.Zlib]
          (writable, readable, inflate)
        },
        { case (writable, readable, inflate) => body(chunkInflater(writable, readable, inflate)) },
        (r, _) => Pull.pure(r._3.close())
      )

    private val emptySlice = Chunk.ArraySlice(Array.empty[Byte], 0, 0)

    private def chunkInflater[F[_]](
        writable: Duplex,
        readable: Readable,
        inflate: zlibMod.Zlib
    )(implicit F: Async[F]): ChunkInflater[F] = {
      var error: Option[js.Error] = None
      var ended: Boolean = false
      val print = true

      val onError: js.Function1[Any, Unit] = { e =>
        if (print) println(s"  . readable.error: ${e}")
        error = e.asInstanceOf[js.Error].some
      }
      val onEnd: js.Function1[Any, Unit] = { _ =>
        if (print) println(s"  . readable.end")
        ended = true
      }
      val onReadable: js.Function1[Any, Unit] = { _ =>
        if (print) println(s"  . readable.readable")
      }

      readable.on("error", onError)
      readable.on("end", onEnd)
      readable.on("readable", onReadable)

      var bytesSent = 0
//      var writtenBefore = 0L

      var latestChunks: Seq[Chunk[Byte]] = Seq.empty

      def chunkSent(chunk: Chunk[Byte]): Unit = {
        bytesSent = bytesSent + chunk.size
        if (print) println(s"  bytes sent: (+${chunk.size}) $bytesSent")
        val bytesWritten = inflate.bytesWritten.toLong
        if (print) println(s"  bytes written: $bytesWritten")
        val bytesToKeep = bytesSent - bytesWritten
        if (print) println(s"  bytes to keep: $bytesToKeep")
        if (bytesToKeep <= chunk.size) {
          latestChunks = Seq(chunk)
        } else {
          latestChunks = latestChunks.inits.toSeq
            .findLast(init => init.map(_.size).sum >= bytesToKeep - chunk.size)
            .getOrElse(Seq.empty) :+ chunk
        }
        if (print) println(s"  keeping chunks: ${latestChunks.size}")
      }

      def remainingChunk(lastChunk: Chunk.ArraySlice[Byte]): Chunk.ArraySlice[Byte] = {
        val bytesWritten = inflate.bytesWritten.toLong
        if (print) println(s"  [remaining] bytes written: $bytesWritten")
        val bytesToKeep = bytesSent - bytesWritten
        if (print) println(s"  [remaining] bytes to keep: $bytesToKeep")
        Chunk.concat(latestChunks :+ lastChunk).takeRight(bytesToKeep.toInt).toArraySlice
      }

      new ChunkInflater[F] {
        def end: Pull[F, INothing, Unit] = Pull.pure {
          if (print) println(s"got end")
          writable.end()
        }

        def inflateChunk(
            bytesChunk: Chunk.ArraySlice[Byte]
        ): Pull[
          F,
          INothing,
          (
              Array[Byte],
              Int,
              Chunk.ArraySlice[Byte],
              Boolean
          ) // (inflatedBuffer, inflatedBytes, remainingBytes, finished)
        ] =
          error match {
            case Some(e) => Pull.raiseError(JavaScriptException(e))
            case None =>
              Pull.eval {
                F.async_[(Array[Byte], Int, Chunk.ArraySlice[Byte], Boolean)] { cb =>
                  if (print)
                    println(
                      s"got chunk to inflate: ${bytesChunk.size} bytes"
                    )
//              val writtenBefore = inflate.bytesWritten.toLong
//              if (print) println(s"  bytes written before: ${writtenBefore}")
                  readable.read() match {
                    case null =>
                      if (print) println(s"  read null; error: $error, ended: $ended")
                      val writtenNow = inflate.bytesWritten.toLong
//                      val bytesWriten = writtenNow - writtenBefore
//                      val bytesRemaining = bytesChunk.size - offset - bytesWriten
//                      writtenBefore = writtenNow

//                      if (print) println(s"  bytes consumed: ${bytesWriten}")

                      if (ended) {
                        cb((Array.empty[Byte], 0, remainingChunk(bytesChunk), true).asRight)
                      } else {
                        if (bytesChunk.nonEmpty) {
                          val buffer = bytesChunk.toUint8Array
                          writable.write(
                            buffer,
                            e =>
                              if (!js.isUndefined(e)) {
                                if (error.isEmpty) {
                                  error = e.asInstanceOf[js.Error].some
                                }
                              }
                          )
                          chunkSent(bytesChunk)
                        }
                        cb((Array.empty[Byte], 0, emptySlice, false).asRight)
                      }

                    case notNull =>
                      val buffer = notNull.asInstanceOf[Buffer]
                      val chunk = buffer.toChunk
                      if (print) println(s"  read buffer: ${chunk.size} bytes")

//                      val writtenNow = inflate.bytesWritten.toLong
//                      val bytesWriten = writtenNow - writtenBefore
//                      val bytesRemaining = bytesChunk.size - offset - bytesWriten
//                      writtenBefore = writtenNow
//                      if (print) println(s"  bytes consumed: ${bytesWriten}")

                      val slice = chunk.toArraySlice
                      cb((slice.values, slice.length, bytesChunk, false).asRight)
                  }

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
