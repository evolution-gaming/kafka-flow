package com.evolutiongaming.kafka.flow.persistence.compression

import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import com.evolutiongaming.skafka.{FromBytes, ToBytes}
import scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

trait Compressor[F[_]] {

  def to(payload: ByteVector): F[ByteVector]

  def from(bytes: ByteVector): F[ByteVector]

}

object Compressor {

  private val uncompressedJsonStartBytes = ByteVector.view("{".getBytes(StandardCharsets.UTF_8))

  def empty[F[_]: Applicative]: Compressor[F] = new Compressor[F] {
    def to(payload: ByteVector): F[ByteVector] = payload.pure[F]
    def from(bytes: ByteVector): F[ByteVector] = bytes.pure[F]
  }

  /** Implements compression using L4Z algorithm. The layout of resulting byte vector is as follows:
    * {{{
    * |---------------|-----------------|------------------|
    * | h_len (int32) | header (byte[]) | payload (byte[]) |
    * |---------------|-----------------|------------------|
    * }}}
    * where:
    *   - h_len is a length of a serialized header
    *   - header is a `com.evolution.aggregator.compression.Header` serialized to JSON and then to byte array with the
    *     flag indicating whether the payload is compressed
    *   - payload is a user payload, either compressed or as-is (see the details below)
    *
    * If the payload size is below the specified threshold, it's not compressed and left as-is with the header
    * indicating that. Otherwise, the payload is compressed on writing and decompressed on reading.
    *
    * When a payload is compressed, it looks like the following:
    * {{{
    * |---------------|------------------|
    * | p_len (int32) | payload (byte[]) |
    * |---------------|------------------|
    * }}}
    * where:
    *   - p_len is a length of the decompressed payload
    *   - payload is a compressed user payload
    *
    * This implementation supports backward-compatible decoding from a serialized JSON, meaning that it will not try to
    * parse byte array to the aforementioned layout if the first bytes of it contain byte representation of a symbol '{'
    */
  def of[F[_]: MonadThrow](
    compressionThreshold: Int = 10000
  )(implicit headerToBytes: ToBytes[F, Header], headerFromBytes: FromBytes[F, Header]): F[Compressor[F]] =
    for {
      compression <- Compression.lz4()
    } yield {
      new Compressor[F] {

        def to(payload: ByteVector): F[ByteVector] = {
          for {
            result <-
              if (payload.length >= compressionThreshold) {
                compression.compress(payload).map((true, _))
              } else {
                (false, payload).pure[F]
              }
            (compressed, compressedBytes) = result
            headerBytes                  <- headerToBytes.apply(Header(compressed = compressed), topic = "")
            bytes                        <- HeaderAndPayload.toBytes[F](ByteVector.view(headerBytes), compressedBytes)
          } yield bytes
        }

        def from(bytes: ByteVector): F[ByteVector] = {
          if (bytes.startsWith(uncompressedJsonStartBytes)) {
            bytes.pure[F]
          } else {
            for {
              result                <- HeaderAndPayload.fromBytes(bytes)
              (headerBytes, payload) = result
              header                <- headerFromBytes.apply(headerBytes.toArray, topic = "")
              bytes <-
                if (header.compressed) {
                  compression.decompress(payload)
                } else {
                  payload.pure[F]
                }
            } yield bytes
          }
        }

      }
    }
}
