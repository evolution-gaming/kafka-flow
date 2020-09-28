package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.syntax.all._

case class Tick[F[_], S](run: S => F[S]) {

  /** Alias for `run` */
  def apply(s: S): F[S] = run(s)

}
object Tick {

  def unit[F[_]: Applicative, S] = Tick[F, S](_.pure[F])

}