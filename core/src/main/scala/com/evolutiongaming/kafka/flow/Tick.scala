package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.syntax.all._

case class Tick[F[_], S](run: S => F[S]) {

  /** Alias for `run` */
  def apply(s: S): F[S] = run(s)

  /** Constructs `Fold` which ignores all incoming `A` elements and just triggers underlying `run` */
  def toFold[A]: Fold[F, S, A] = Fold { (s, _) =>
    run(s)
  }

}
object Tick {

  def unit[F[_]: Applicative, S] = Tick[F, S](_.pure[F])

}