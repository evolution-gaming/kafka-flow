package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.syntax.all._
import cats.Functor

case class Tick[F[_], S](run: S => F[S]) {

  /** Alias for `run` */
  def apply(s: S): F[S] = run(s)

  /** Transforms the state `S` of the `Tick` to something else.
    *
    * It is possible to use additional information from previous `T` to build a new state.
    *
    * The common use for this method is to augument original state with some metainformation, i.e. offset or sequence
    * number.
    *
    * See also `StateT#transformS` for more details.
    */
  def expand[T](f: T => S)(g: (S, T) => T)(implicit F: Functor[F]): Tick[F, T] =
    Tick { t =>
      run(f(t)) map (g(_, t))
    }

  /** Constructs `Fold` which ignores all incoming `A` elements and just triggers underlying `run` */
  def toFold[A]: Fold[F, S, A] = Fold { (s, _) =>
    run(s)
  }

}
object Tick {

  /** Does nothing to the state */
  def id[F[_]: Applicative, S]: Tick[F, S] = Tick[F, S](_.pure[F])

}
