package com.evolutiongaming.kafka.flow

import cats.Applicative

case class TickOption[F[_], S](value: Tick[F, Option[S]]) {

  /** Alias for `value.run` */
  def apply(s: Option[S]): F[Option[S]] = value(s)

  /** Constructs `FoldOption` which ignores all incoming `A` elements and just triggers underlying `value.run` */
  def toFold[A]: FoldOption[F, S, A] = FoldOption(value.toFold[A])

}
object TickOption {

  def of[F[_], S](run: Option[S] => F[Option[S]]): TickOption[F, S] =
    TickOption(Tick(run))

  def unit[F[_]: Applicative, S]: TickOption[F, S] = TickOption(Tick.unit)

}