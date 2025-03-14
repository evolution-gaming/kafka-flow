package com.evolutiongaming.kafka.flow.persistence.compression

import cats.MonadThrow
import cats.syntax.all.*
import scodec.Attempt
import scodec.bits.ByteVector
import scodec.codecs.*

private[compression] object HeaderAndPayload {

  private val codec = variableSizeBytes(int32, bytes) ~ bytes

  def toBytes[F[_]: MonadThrow](header: ByteVector, payload: ByteVector): F[ByteVector] = {
    codec.encode((header, payload)) match {
      case Attempt.Successful(value) => value.bytes.pure[F]
      case Attempt.Failure(e)        => CompressionError(e.message).raiseError
    }
  }

  def fromBytes[F[_]: MonadThrow](bytes: ByteVector): F[(ByteVector, ByteVector)] = {
    codec.decode(bytes.bits) match {
      case Attempt.Successful(value) => value.value.pure[F]
      case Attempt.Failure(e)        => CompressionError(e.message).raiseError
    }
  }
}
