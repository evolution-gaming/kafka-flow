package com.evolutiongaming.kafka.flow.persistence.compression

import scodec.bits.ByteVector

final case class Header(compressed: Boolean)

trait HeaderCodec[F[_]] {
  def fromBytes(bytes: ByteVector): F[Option[Header]]

  def toBytes(header: Header): F[ByteVector]
}
