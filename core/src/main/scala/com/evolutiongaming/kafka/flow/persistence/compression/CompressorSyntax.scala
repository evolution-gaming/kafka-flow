package com.evolutiongaming.kafka.flow.persistence.compression

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.skafka.{Bytes, FromBytes, ToBytes, Topic}
import scodec.bits.ByteVector

object CompressorSyntax {
  implicit class ToBytesWithCompressionOps[F[_], A](val self: ToBytes[F, A]) extends AnyVal {
    def withCompression(compressor: Compressor[F])(implicit m: Monad[F]): ToBytes[F, A] =
      new ToBytesWithCompression[F, A](self, compressor)
  }

  implicit class FromBytesWithCompressionOps[F[_], A](val self: FromBytes[F, A]) extends AnyVal {
    def withCompression(compressor: Compressor[F])(implicit m: Monad[F]): FromBytes[F, A] =
      new FromBytesWithCompression[F, A](self, compressor)
  }

  private final class ToBytesWithCompression[F[_]: Monad, A](
    self: ToBytes[F, A],
    compressor: Compressor[F]
  ) extends ToBytes[F, A] {
    def apply(a: A, topic: Topic): F[Bytes] =
      for {
        bytes           <- self(a, topic)
        compressedBytes <- compressor.to(ByteVector.view(bytes))
      } yield compressedBytes.toArray
  }

  private final class FromBytesWithCompression[F[_]: Monad, A](
    self: FromBytes[F, A],
    compressor: Compressor[F]
  ) extends FromBytes[F, A] {
    def apply(bytes: Bytes, topic: Topic): F[A] =
      for {
        decompressed <- compressor.from(ByteVector.view(bytes))
        value        <- self(decompressed.toArray, topic)
      } yield value
  }
}
