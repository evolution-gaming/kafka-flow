package com.evolutiongaming.kafka.flow

import cats.Applicative

case class TickOption[F[_], S](value: Tick[F, Option[S]]) {

  /** Alias for `value.run` */
  def apply(s: Option[S]): F[Option[S]] = value(s)

}
object TickOption {

  def unit[F[_]: Applicative, S]: TickOption[F, S] = TickOption(Tick.unit)

}