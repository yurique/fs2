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
package compression

import cats.effect.{Ref, Sync}
import cats.syntax.all._
import fs2.compression.internal._

import java.io.EOFException
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

class Gzip[F[_]](implicit F: Sync[F]) {

  def gzip(
      fileName: Option[String],
      modificationTime: Option[FiniteDuration],
      comment: Option[String],
      deflate: Pipe[F, Byte, Byte],
      deflateParams: DeflateParams,
      osName: String
  ): Pipe[F, Byte, Byte] =
    stream =>
      deflateParams match {
        case params: DeflateParams if params.header == ZLibParams.Header.GZIP =>
          Stream.eval(Ref.of[F, Long](0)).flatMap { bytesIn =>
            val crc = new CRC32
            _gzip_header(
              fileName,
              modificationTime,
              comment,
              params.level.juzDeflaterLevel,
              params.fhCrcEnabled,
              osName
            ) ++
              stream
                .through(CrcPipe(crc))
                .through(CountPipe(bytesIn))
                .through(deflate) ++
              _gzip_trailer(bytesIn, crc)
          }

        case params: DeflateParams =>
          Stream.raiseError[F](
            new ZipException(
              s"${ZLibParams.Header.GZIP} header type required, not ${params.header}."
            )
          )
      }

  private def _gzip_header(
      fileName: Option[String],
      modificationTime: Option[FiniteDuration],
      comment: Option[String],
      deflateLevel: Int,
      fhCrcEnabled: Boolean,
      osName: String
  ): Stream[F, Byte] = {
    // See RFC 1952: https://www.ietf.org/rfc/rfc1952.txt
    val secondsSince197001010000: Long =
      modificationTime.map(_.toSeconds).getOrElse(0)
    val header = Array[Byte](
      gzipMagicFirstByte, // ID1: Identification 1
      gzipMagicSecondByte, // ID2: Identification 2
      gzipCompressionMethod.DEFLATE, // CM: Compression Method
      ((if (fhCrcEnabled) gzipFlag.FHCRC else zeroByte) + // FLG: Header CRC
        fileName.map(_ => gzipFlag.FNAME).getOrElse(zeroByte) + // FLG: File name
        comment.map(_ => gzipFlag.FCOMMENT).getOrElse(zeroByte)).toByte, // FLG: Comment
      (secondsSince197001010000 & 0xff).toByte, // MTIME: Modification Time
      ((secondsSince197001010000 >> 8) & 0xff).toByte,
      ((secondsSince197001010000 >> 16) & 0xff).toByte,
      ((secondsSince197001010000 >> 24) & 0xff).toByte,
      deflateLevel match { // XFL: Extra flags
        case 9 => gzipExtraFlag.DEFLATE_MAX_COMPRESSION_SLOWEST_ALGO
        case 1 => gzipExtraFlag.DEFLATE_FASTEST_ALGO
        case _ => zeroByte
      },
      gzipOperatingSystem.THIS(osName)
    ) // OS: Operating System

    val crc32 = new CRC32()
    crc32.update(header)

    val fileNameEncoded = fileName.map { string =>
      val bytes = string.replaceAll("\u0000", "_").getBytes(StandardCharsets.ISO_8859_1)
      crc32.update(bytes)
      crc32.update(zeroByte.toInt)
      bytes
    }
    val commentEncoded = comment.map { string =>
      val bytes = string.replaceAll("\u0000", " ").getBytes(StandardCharsets.ISO_8859_1)
      crc32.update(bytes)
      crc32.update(zeroByte.toInt)
      bytes
    }
    val crc32Value = crc32.getValue()

    val crc16 =
      if (fhCrcEnabled)
        Array[Byte](
          (crc32Value & 0xff).toByte,
          ((crc32Value >> 8) & 0xff).toByte
        )
      else
        Array.emptyByteArray

    Stream.chunk(moveAsChunkBytes(header)) ++
      fileNameEncoded
        .map(bytes => Stream.chunk(moveAsChunkBytes(bytes)) ++ Stream.emit(zeroByte))
        .getOrElse(Stream.empty) ++
      commentEncoded
        .map(bytes => Stream.chunk(moveAsChunkBytes(bytes)) ++ Stream.emit(zeroByte))
        .getOrElse(Stream.empty) ++
      Stream.chunk(moveAsChunkBytes(crc16))
  }

  private def _gzip_trailer(bytesIn: Ref[F, Long], CRC32: CRC32): Stream[F, Byte] =
    Stream.eval(bytesIn.get).flatMap { bytesIn =>
      // See RFC 1952: https://www.ietf.org/rfc/rfc1952.txt
      val crc = CRC32.getValue() & 0xffffffff
      val crc32Value = crc
      val trailer = Array[Byte](
        (crc32Value & 0xff).toByte, // CRC-32: Cyclic Redundancy Check
        ((crc32Value >> 8) & 0xff).toByte,
        ((crc32Value >> 16) & 0xff).toByte,
        ((crc32Value >> 24) & 0xff).toByte,
        (bytesIn & 0xff).toByte, // ISIZE: Input size
        ((bytesIn >> 8) & 0xff).toByte,
        ((bytesIn >> 16) & 0xff).toByte,
        ((bytesIn >> 24) & 0xff).toByte
      )
      Stream.chunk(moveAsChunkBytes(trailer))
    }

  def gunzip(
      inflate: Stream[F, Byte] => Stream[
        F,
        (Stream[F, Byte], Ref[F, Chunk[Byte]], Ref[F, Long], Ref[F, Long])
      ],
      inflateParams: InflateParams
  ): Stream[F, Byte] => Stream[F, GunzipResult[F]] =
    stream =>
      inflateParams match {
        case params: InflateParams if params.header == ZLibParams.Header.GZIP =>
          stream.pull
            .unconsN(gzipHeaderBytes)
            .flatMap {
              case Some((mandatoryHeaderChunk, streamAfterMandatoryHeader)) =>
                _gunzip_matchMandatoryHeader(
                  mandatoryHeaderChunk,
                  streamAfterMandatoryHeader,
                  inflate
                )
              case None =>
                Pull.output1(GunzipResult.create(Stream.raiseError(new EOFException())))
            }
            .stream

        case params: InflateParams =>
          Stream.raiseError(
            new ZipException(
              s"${ZLibParams.Header.GZIP} header type required, not ${params.header}."
            )
          )
      }

  private def _gunzip_matchMandatoryHeader(
      mandatoryHeaderChunk: Chunk[Byte],
      streamAfterMandatoryHeader: Stream[F, Byte],
      inflate: Stream[F, Byte] => Stream[
        F,
        (Stream[F, Byte], Ref[F, Chunk[Byte]], Ref[F, Long], Ref[F, Long])
      ]
  ) = {
    (mandatoryHeaderChunk.size, mandatoryHeaderChunk.toArraySlice.values) match {
      case (
            `gzipHeaderBytes`,
            Array(
              `gzipMagicFirstByte`,
              `gzipMagicSecondByte`,
              gzipCompressionMethod.DEFLATE,
              flags,
              _,
              _,
              _,
              _,
              _
            )
          ) if gzipFlag.reserved5(flags) =>
        Pull.output1(
          GunzipResult.create(
            Stream.raiseError(
              new ZipException("Unsupported gzip flag reserved bit 5 is non-zero")
            )
          )
        )
      case (
            `gzipHeaderBytes`,
            Array(
              `gzipMagicFirstByte`,
              `gzipMagicSecondByte`,
              gzipCompressionMethod.DEFLATE,
              flags,
              _,
              _,
              _,
              _,
              _
            )
          ) if gzipFlag.reserved6(flags) =>
        Pull.output1(
          GunzipResult.create(
            Stream.raiseError(
              new ZipException("Unsupported gzip flag reserved bit 6 is non-zero")
            )
          )
        )
      case (
            `gzipHeaderBytes`,
            Array(
              `gzipMagicFirstByte`,
              `gzipMagicSecondByte`,
              gzipCompressionMethod.DEFLATE,
              flags,
              _,
              _,
              _,
              _,
              _
            )
          ) if gzipFlag.reserved7(flags) =>
        Pull.output1(
          GunzipResult.create(
            Stream.raiseError(
              new ZipException("Unsupported gzip flag reserved bit 7 is non-zero")
            )
          )
        )
      case (
            `gzipHeaderBytes`,
            header @ Array(
              `gzipMagicFirstByte`,
              `gzipMagicSecondByte`,
              gzipCompressionMethod.DEFLATE,
              flags,
              _,
              _,
              _,
              _,
              _,
              _
            )
          ) =>
        val headerCrc32 = new CRC32
        headerCrc32.update(header)
        val secondsSince197001010000 =
          unsignedToLong(header(4), header(5), header(6), header(7))
        _gunzip_readOptionalHeader(
          streamAfterMandatoryHeader,
          flags,
          headerCrc32,
          secondsSince197001010000,
          inflate
        ).pull.uncons1
          .flatMap {
            case Some((gunzipResult, _)) =>
              Pull.output1(gunzipResult)
            case None =>
              Pull.output1(GunzipResult.create(Stream.raiseError(new EOFException())))
          }
      case (
            `gzipHeaderBytes`,
            Array(
              `gzipMagicFirstByte`,
              `gzipMagicSecondByte`,
              compressionMethod,
              _,
              _,
              _,
              _,
              _,
              _,
              _
            )
          ) =>
        Pull.output1(
          GunzipResult.create(
            Stream.raiseError(
              new ZipException(
                s"Unsupported gzip compression method: $compressionMethod"
              )
            )
          )
        )
      case _ =>
        Pull.output1(
          GunzipResult.create(Stream.raiseError(new ZipException("Not in gzip format")))
        )
    }
  }

  private def _gunzip_readOptionalHeader(
      streamAfterMandatoryHeader: Stream[F, Byte],
      flags: Byte,
      headerCrc32: CRC32,
      secondsSince197001010000: Long,
      inflate: Stream[F, Byte] => Stream[
        F,
        (Stream[F, Byte], Ref[F, Chunk[Byte]], Ref[F, Long], Ref[F, Long])
      ]
  ): Stream[F, GunzipResult[F]] =
    _gunzip_skipOptionalExtraField(gzipFlag.fextra(flags), headerCrc32)(streamAfterMandatoryHeader)
      .flatMap { streamAfterOptionalExtraField =>
        streamAfterOptionalExtraField
          .through(
            _gunzip_readOptionalStringField(
              gzipFlag.fname(flags),
              headerCrc32,
              "file name",
              fileNameBytesSoftLimit
            )
          )
          .flatMap { case (fileName, streamAfterFileName) =>
            streamAfterFileName
              .through(
                _gunzip_readOptionalStringField(
                  gzipFlag.fcomment(flags),
                  headerCrc32,
                  "file comment",
                  fileCommentBytesSoftLimit
                )
              )
              .flatMap { case (comment, streamAfterComment) =>
                Stream.emit(
                  GunzipResult.create(
                    modificationTimeEpoch =
                      if (secondsSince197001010000 != 0)
                        Some(FiniteDuration(secondsSince197001010000, TimeUnit.SECONDS))
                      else None,
                    fileName = fileName,
                    comment = comment,
                    content = inflate(
                      streamAfterComment
                        .through(
                          _gunzip_validateHeader(
                            (flags & gzipFlag.FHCRC) == gzipFlag.FHCRC,
                            headerCrc32
                          )
                        )
                    ).flatMap { case (inflated, trailerChunk, bytesWritten, crc32) =>
                      inflated ++
                        _gunzip_validateTrailer(
                          trailerStream = trailerChunk,
                          crc32 = crc32,
                          bytesWritten = bytesWritten
                        )
                    }
                  )
                )
              }
          }
      }

  private def _gunzip_skipOptionalExtraField(
      isPresent: Boolean,
      crc32Builder: CRC32
  ): Stream[F, Byte] => Stream[F, Stream[F, Byte]] =
    stream =>
      if (isPresent) {
        stream.pull
          .unconsN(gzipOptionalExtraFieldLengthBytes)
          .flatMap {
            case Some((optionalExtraFieldLengthChunk, streamAfterOptionalExtraFieldLength)) =>
              (
                optionalExtraFieldLengthChunk.size,
                optionalExtraFieldLengthChunk.toArraySlice.values
              ) match {
                case (
                      `gzipOptionalExtraFieldLengthBytes`,
                      lengthBytes @ Array(firstByte, secondByte)
                    ) =>
                  val optionalExtraFieldLength = unsignedToInt(firstByte, secondByte)
                  streamAfterOptionalExtraFieldLength.pull
                    .unconsN(optionalExtraFieldLength)
                    .flatMap {
                      case Some((optionalExtraFieldChunk, streamAfterOptionalExtraField)) =>
                        crc32Builder.update(lengthBytes)
                        crc32Builder.updateChunk(
                          optionalExtraFieldChunk
                        )
                        Pull.output1(streamAfterOptionalExtraField)
                      case None =>
                        Pull.raiseError(
                          new ZipException("Failed to read optional extra field header")
                        )
                    }

                case _ =>
                  Pull.raiseError(
                    new ZipException("Failed to read optional extra field header length")
                  )
              }
            case None =>
              Pull.raiseError(new EOFException())
          }
          .stream
      } else Stream(stream)

  private def _gunzip_readOptionalStringField(
      isPresent: Boolean,
      headerCrc32: CRC32,
      fieldName: String,
      fieldBytesSoftLimit: Int
  ): Stream[F, Byte] => Stream[F, (Option[String], Stream[F, Byte])] =
    stream =>
      if (isPresent)
        UnconsUntil[F](zeroByte, fieldBytesSoftLimit, headerCrc32)
          .apply(stream)
          .flatMap {
            case Some((chunk, rest)) =>
              Pull.output1(
                (
                  if (chunk.size <= 1)
                    Some("")
                  else {
                    val bytesChunk = chunk.toArraySlice
                    Some(
                      new String(
                        bytesChunk.values,
                        bytesChunk.offset,
                        bytesChunk.length - 1,
                        StandardCharsets.ISO_8859_1
                      )
                    )
                  },
                  rest
                )
              )
            case None =>
              Pull.output1(
                (
                  Option.empty[String],
                  Stream.raiseError(new ZipException(s"Failed to read $fieldName field"))
                )
              )
          }
          .stream
      else Stream.emit((Option.empty[String], stream))

  private def _gunzip_validateHeader(
      isPresent: Boolean,
      headerCrc32: CRC32
  ): Pipe[F, Byte, Byte] =
    stream =>
      if (isPresent)
        stream.pull
          .unconsN(gzipHeaderCrcBytes)
          .flatMap {
            case Some((headerCrcChunk, streamAfterHeaderCrc)) =>
              val expectedHeaderCrc16 = unsignedToInt(headerCrcChunk(0), headerCrcChunk(1))
              val actualHeaderCrc16 = headerCrc32.getValue().toInt & 0xffff
              if (expectedHeaderCrc16 != actualHeaderCrc16)
                Pull.raiseError(new ZipException("Header failed CRC validation"))
              else
                Pull.output1(streamAfterHeaderCrc)
            case None =>
              Pull.raiseError(new ZipException("Failed to read header CRC"))
          }
          .stream
          .flatten
      else stream

  private def _gunzip_validateTrailer(
      trailerStream: Ref[F, Chunk[Byte]],
      crc32: Ref[F, Long],
      bytesWritten: Ref[F, Long]
  ): Stream[F, Byte] =
    Stream
      .eval {
        (trailerStream.get, crc32.get, bytesWritten.get).tupled.flatMap[Unit] {
          case (trailerChunk, crc32, actualInputSize) =>
            if (trailerChunk.size != gzipTrailerBytes) {
              F.raiseError(new ZipException(s"Failed to read trailer (1): $trailerChunk"))
            } else {
              val expectedInputCrc32 =
                unsignedToLong(trailerChunk(0), trailerChunk(1), trailerChunk(2), trailerChunk(3))

              val actualInputCrc32 = crc32

              val expectedInputSize =
                unsignedToLong(trailerChunk(4), trailerChunk(5), trailerChunk(6), trailerChunk(7))

              if (expectedInputCrc32 != actualInputCrc32) {
                F.raiseError[Unit](
                  new ZipException(
                    s"Content failed CRC validation: expected $expectedInputCrc32 != actual $actualInputCrc32"
                  )
                )
              } else if (expectedInputSize != actualInputSize) {
                F.raiseError[Unit](new ZipException("Content failed size validation"))
              } else {
                F.unit
              }
            }
        }
      }
      .flatMap(_ => Stream.empty)

  private final val gzipHeaderBytes = 10
  private final val gzipMagicFirstByte: Byte = 0x1f.toByte
  private final val gzipMagicSecondByte: Byte = 0x8b.toByte
  private object gzipCompressionMethod {
    val DEFLATE: Byte = 8.toByte // Deflater.DEFLATED.toByte
  }
  private object gzipFlag {
    def apply(flags: Byte, flag: Byte): Boolean = (flags & flag) == flag
    def apply(flags: Byte, flag: Int): Boolean = (flags & flag) == flag

    def ftext(flags: Byte): Boolean = apply(flags, FTEXT)
    def fhcrc(flags: Byte): Boolean = apply(flags, FHCRC)
    def fextra(flags: Byte): Boolean = apply(flags, FEXTRA)
    def fname(flags: Byte): Boolean = apply(flags, FNAME)
    def fcomment(flags: Byte): Boolean = apply(flags, FCOMMENT)
    def reserved5(flags: Byte): Boolean = apply(flags, RESERVED_BIT_5)
    def reserved6(flags: Byte): Boolean = apply(flags, RESERVED_BIT_6)
    def reserved7(flags: Byte): Boolean = apply(flags, RESERVED_BIT_7)

    final val FTEXT: Byte = 1
    final val FHCRC: Byte = 2
    final val FEXTRA: Byte = 4
    final val FNAME: Byte = 8
    final val FCOMMENT: Byte = 16
    final val RESERVED_BIT_5 = 32
    final val RESERVED_BIT_6 = 64
    final val RESERVED_BIT_7: Int = 128
  }
  private object gzipExtraFlag {
    val DEFLATE_MAX_COMPRESSION_SLOWEST_ALGO: Byte = 2
    val DEFLATE_FASTEST_ALGO: Byte = 4
  }
  private final val gzipOptionalExtraFieldLengthBytes = 2
  private final val gzipHeaderCrcBytes = 2
  private object gzipOperatingSystem {
    final val FAT_FILESYSTEM: Byte = 0
    final val AMIGA: Byte = 1
    final val VMS: Byte = 2
    final val UNIX: Byte = 3
    final val VM_CMS: Byte = 4
    final val ATARI_TOS: Byte = 5
    final val HPFS_FILESYSTEM: Byte = 6
    final val MACINTOSH: Byte = 7
    final val Z_SYSTEM: Byte = 8
    final val CP_M: Byte = 9
    final val TOPS_20: Byte = 10
    final val NTFS_FILESYSTEM: Byte = 11
    final val QDOS: Byte = 12
    final val ACORN_RISCOS: Byte = 13
    final val UNKNOWN: Byte = 255.toByte

    def THIS(osName: String): Byte = osName match {
      case null => UNKNOWN
      case s =>
        s.toLowerCase() match {
          case name if name.indexOf("nux") > 0     => UNIX
          case name if name.indexOf("nix") > 0     => UNIX
          case name if name.indexOf("aix") >= 0    => UNIX
          case name if name.indexOf("bsd") >= 0    => UNIX
          case name if name.indexOf("cygwin") >= 0 => NTFS_FILESYSTEM
          case name if name.indexOf("win") >= 0    => NTFS_FILESYSTEM
          case name if name.indexOf("mac") >= 0    => MACINTOSH
          case name if name.indexOf("darwin") >= 0 => MACINTOSH
          case _                                   => UNKNOWN
        }
    }
  }
  private final val gzipInputCrcBytes = 4
  private final val gzipInputSizeBytes = 4
  final val gzipTrailerBytes: Int = gzipInputCrcBytes + gzipInputSizeBytes

  private final val zeroByte: Byte = 0

  private final val fileNameBytesSoftLimit =
    1024 // A limit is good practice. Actual limit will be max(chunk.size, soft limit). Typical maximum file size is 255 characters.
  private final val fileCommentBytesSoftLimit =
    1024 * 1024 // A limit is good practice. Actual limit will be max(chunk.size, soft limit). 1 MiB feels reasonable for a comment.

  private def moveAsChunkBytes(values: Array[Byte]): Chunk[Byte] =
    moveAsChunkBytes(values, values.length)

  private def moveAsChunkBytes(values: Array[Byte], length: Int): Chunk[Byte] =
    if (length > 0) Chunk.array(values, 0, length)
    else Chunk.empty[Byte]

  private def unsignedToInt(lsb: Byte, msb: Byte): Int =
    ((msb & 0xff) << 8) | (lsb & 0xff)

  private def unsignedToLong(lsb: Byte, byte2: Byte, byte3: Byte, msb: Byte): Long =
    ((msb.toLong & 0xff) << 24) | ((byte3 & 0xff) << 16) | ((byte2 & 0xff) << 8) | (lsb & 0xff)

}
