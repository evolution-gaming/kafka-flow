package com.evolution.kafka.flow.key

import cats.syntax.all.*
import com.evolution.scassandra.EncodeByName

private[flow] sealed abstract case class SegmentNr(value: Long) {
  override def toString: String = value.toString
}

private[flow] object SegmentNr {

  val min: SegmentNr = new SegmentNr(0L) {}

  val max: SegmentNr = new SegmentNr(Long.MaxValue) {}

  implicit val encodeByNameSegmentNr: EncodeByName[SegmentNr] = EncodeByName[Long].contramap(_.value)

  def of(value: Long): Either[String, SegmentNr] = value match {
    case value if value < min.value =>
      s"invalid SegmentNr of $value, it must be greater or equal to $min".asLeft[SegmentNr]
    case value if value > max.value =>
      s"invalid SegmentNr of $value, it must be less or equal to $max".asLeft[SegmentNr]
    case value if value === min.value => min.asRight[String]
    case value if value === max.value => max.asRight[String]
    case value                        => new SegmentNr(value) {}.asRight[String]
  }
}
