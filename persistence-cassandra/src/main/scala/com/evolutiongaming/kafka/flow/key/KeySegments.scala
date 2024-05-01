package com.evolutiongaming.kafka.flow.key

import cats.Show
import cats.kernel.{Eq, Order}
import cats.syntax.all._

/** A maximum number of segments in Cassandra table.
  *
  * When [[KeySegments]] is used, the value of 'segment' column is determined by consistent hashing of the key column.
  * I.e. there always no more than [[KeySegments#value]] different values.
  *
  * The only place where such approach is used right now is [[com.evolutiongaming.kafka.flow.key.CassandraKeys]]. This
  * allows fair distribution of 'key' records between the Cassandra partitions.
  */
sealed abstract case class KeySegments(value: Int) {

  override def toString: String = value.toString
}

object KeySegments {

  val min: KeySegments = new KeySegments(1) {}

  val max: KeySegments = new KeySegments(Int.MaxValue) {}

  val default: KeySegments = new KeySegments(10000) {}

  implicit val eqKeySegments: Eq[KeySegments] = Eq.fromUniversalEquals

  implicit val showKeySegments: Show[KeySegments] = Show.fromToString

  implicit val orderingKeySegments: Ordering[KeySegments] = Ordering.by(_.value)

  implicit val orderKeySegments: Order[KeySegments] = Order.fromOrdering

  def of(value: Int): Either[String, KeySegments] = {
    if (value < min.value) {
      Left(s"invalid KeySegments of $value, it must be greater or equal to $min")
    } else if (value > max.value) {
      Left(s"invalid KeySegments of $value, it must be less or equal to $max")
    } else if (value === min.value) {
      Right(min)
    } else if (value === max.value) {
      Right(max)
    } else {
      Right(new KeySegments(value) {})
    }
  }

  def opt(value: Int): Option[KeySegments] = of(value).toOption

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): KeySegments =
    of(numeric.toInt(value)).fold(err => throw new RuntimeException(err), identity)
}
