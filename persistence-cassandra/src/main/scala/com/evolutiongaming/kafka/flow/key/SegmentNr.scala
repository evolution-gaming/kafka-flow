package com.evolutiongaming.kafka.flow.key

import cats.syntax.all._
import com.evolutiongaming.scassandra.EncodeByName

private[flow] sealed abstract case class SegmentNr(value: Long) {
  override def toString: String = value.toString
}

private[flow] object SegmentNr {

  val min: SegmentNr = new SegmentNr(0L) {}

  val max: SegmentNr = new SegmentNr(Long.MaxValue) {}

  implicit val encodeByNameSegmentNr: EncodeByName[SegmentNr] = EncodeByName[Long].contramap(_.value)

  def of(value: Long): Either[String, SegmentNr] = {
    if (value < min.value) {
      s"invalid SegmentNr of $value, it must be greater or equal to $min".asLeft[SegmentNr]
    } else if (value > max.value) {
      s"invalid SegmentNr of $value, it must be less or equal to $max".asLeft[SegmentNr]
    } else if (value === min.value) {
      min.asRight[String]
    } else if (value === max.value) {
      max.asRight[String]
    } else {
      new SegmentNr(value) {}.asRight[String]
    }
  }
}
