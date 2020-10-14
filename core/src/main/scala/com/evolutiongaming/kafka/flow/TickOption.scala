package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.Functor

case class TickOption[F[_], S](value: Tick[F, Option[S]]) {

  /** Alias for `value.run` */
  def apply(s: Option[S]): F[Option[S]] = value(s)

  /** Transforms the state `S` of the `Tick` to something else.
    *
    * It is possible to use additional information from previous `T` to
    * build a new state.
    *
    * The common use for this method is to augument original state with
    * some metainformation, i.e. offset or sequence number.
    *
    * See also `StateT#transformS` for more details.
    *
    * If any of `S` or `T` is `None` then the output will also be `None`,
    * though underlying `run` will be called anyway.
    */
  def expand[T](f: T => S)(g: (S, T) => T)(implicit F: Functor[F]): TickOption[F, T] =
    TickOption {
      value.expand[Option[T]](_ map f) { (s, t) =>
        for {
          s <- s
          t <- t
        } yield g(s, t)
      }
    }

  /** Constructs `FoldOption` which ignores all incoming `A` elements and just triggers underlying `value.run` */
  def toFold[A]: FoldOption[F, S, A] = FoldOption(value.toFold[A])

}
object TickOption {

  def of[F[_], S](run: Option[S] => F[Option[S]]): TickOption[F, S] =
    TickOption(Tick(run))

  /** Does nothing to the state */
  def id[F[_]: Applicative, S]: TickOption[F, S] = TickOption(Tick.id)

  @deprecated("Use `id` instead", "0.2.4")
  def unit[F[_]: Applicative, S]: TickOption[F, S] = TickOption(Tick.id)

}