package com.evolutiongaming.kafka.flow.persistence.compression

import cats.MonadThrow
import cats.syntax.all._
import net.jpountz.lz4.LZ4Factory
import scodec.Attempt
import scodec.bits.ByteVector
import scodec.codecs._

import java.nio.ByteBuffer

private[flow] trait Compression[F[_]] {
  def compress(bytes: ByteVector): F[ByteVector]
  def decompress(bytes: ByteVector): F[ByteVector]
}

private[flow] object Compression {

  def lz4[F[_]: MonadThrow](): F[Compression[F]] = {

    for {
      factory <- MonadThrow[F].catchNonFatal(LZ4Factory.fastestInstance())
    } yield {
      val codec = int32 ~ bytes

      val compressor   = factory.fastCompressor()
      val decompressor = factory.fastDecompressor()

      new Compression[F] {

        def compress(bytes0: ByteVector): F[ByteVector] = {
          val bytes              = bytes0.toArray
          val lengthDecompressed = bytes.length
          for {
            bytesCompressed <- MonadThrow[F].catchNonFatal {
              val maxLengthCompressed = compressor.maxCompressedLength(lengthDecompressed)
              val compressed          = new Array[Byte](maxLengthCompressed)
              val lengthCompressed    = compressor.compress(bytes, compressed)
              ByteVector.view(compressed, 0, lengthCompressed)
            }
            bits <- codec.encode((lengthDecompressed, bytesCompressed)) match {
              case Attempt.Successful(value) => value.pure[F]
              case Attempt.Failure(e)        => CompressionError(e.message).raiseError
            }
          } yield bits.bytes
        }

        def decompress(bytes: ByteVector): F[ByteVector] = {
          for {
            decoded <- codec.decode(bytes.bits) match {
              case Attempt.Successful(value) => value.pure[F]
              case Attempt.Failure(e)        => CompressionError(e.message).raiseError
            }
            (lengthDecompressed, byteVector) = decoded.value
            buffer                           = byteVector.toByteBuffer
            decompressed <- MonadThrow[F].catchNonFatal {
              val decompressed = ByteBuffer.allocate(lengthDecompressed)
              decompressor.decompress(buffer, decompressed)
              decompressed
            }
          } yield ByteVector.view(decompressed.array())

        }
      }
    }
  }
}
