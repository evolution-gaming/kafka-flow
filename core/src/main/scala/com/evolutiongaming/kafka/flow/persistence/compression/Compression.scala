package com.evolutiongaming.kafka.flow.persistence.compression

import cats.MonadThrow
import cats.syntax.all._
import net.jpountz.lz4.LZ4Factory
import scodec.Attempt
import scodec.bits.ByteVector
import scodec.codecs._

import java.nio.ByteBuffer

/** Internal implementation of compression */
private[flow] trait Compression[F[_]] {
  def compress(bytes: ByteVector): F[ByteVector]
  def decompress(bytes: ByteVector): F[ByteVector]
}

private[flow] object Compression {
  def lz4[F[_]: MonadThrow](): F[Compression[F]] = {
    for {
      factory <- MonadThrow[F].catchNonFatal(LZ4Factory.fastestInstance())
    } yield new Lz4Compression[F](factory)
  }
}

/** LZ4-based implementation. The layout of the resulting byte vector is as follows:
  * {{{
  * |---------------|------------------|
  * | p_len (int32) | payload (byte[]) |
  * |---------------|------------------|
  * }}}
  * where:
  *   - `p_len` is a length of an uncompressed input byte vector. It's required to allocate the buffer of a proper size
  *     when decompressing is done
  *   - `payload` is a compressed version of the input byte vector
  * @param factory a factory instance which is reusable
  */
private[flow] class Lz4Compression[F[_]: MonadThrow](factory: LZ4Factory) extends Compression[F] {
  private val codec = int32 ~ bytes

  private val compressor = factory.fastCompressor()
  private val decompressor = factory.fastDecompressor()

  override def compress(bytes0: ByteVector): F[ByteVector] = {
    val bytes = bytes0.toArray
    val lengthDecompressed = bytes.length
    for {
      bytesCompressed <- MonadThrow[F].catchNonFatal {
        val maxLengthCompressed = compressor.maxCompressedLength(lengthDecompressed)
        val compressed = new Array[Byte](maxLengthCompressed)
        val lengthCompressed = compressor.compress(bytes, compressed)
        ByteVector.view(compressed, 0, lengthCompressed)
      }
      bits <- codec.encode((lengthDecompressed, bytesCompressed)) match {
        case Attempt.Successful(value) => value.pure[F]
        case Attempt.Failure(e)        => CompressionError(e.message).raiseError
      }
    } yield bits.bytes
  }

  override def decompress(bytes: ByteVector): F[ByteVector] = {
    for {
      decoded <- codec.decode(bytes.bits) match {
        case Attempt.Successful(value) => value.pure[F]
        case Attempt.Failure(e)        => CompressionError(e.message).raiseError
      }
      (lengthDecompressed, byteVector) = decoded.value
      buffer = byteVector.toByteBuffer
      decompressed <- MonadThrow[F].catchNonFatal {
        val decompressed = ByteBuffer.allocate(lengthDecompressed)
        decompressor.decompress(buffer, decompressed)
        decompressed
      }
    } yield ByteVector.view(decompressed.array())
  }
}
