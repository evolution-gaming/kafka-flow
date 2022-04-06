package com.evolutiongaming.kafka.flow

import cats.{Applicative, Monad}

/** A thin wrapper around `FoldOption`, allowing it to use some additional functionality provided by `FoldContext`.
  * A resulting instance of `FoldOption` is created per each individual key when it's recovered (either from
  * persistence layer or when encountered the first time in the source Kafka topic).
  *
  * @tparam F computation effect
  * @tparam S state
  * @tparam E incoming event
  *
  */
trait ContextFold[F[_], S, E] {

  /** Provides the context, returning a non-context-aware `FoldOption` */
  def apply(context: FoldContext[F]): FoldOption[F, S, E]

  final def flatMap(f: S => ContextFold[F, S, E])(implicit F: Monad[F]): ContextFold[F, S, E] =
    context => apply(context).flatMap(s1 => f(s1).apply(context))

  final def filter(f: (S, E) => Boolean)(implicit F: Applicative[F]): ContextFold[F, S, E] =
    context => apply(context).filter(f)
}

object ContextFold {

  def empty[F[_], S, E](implicit F: Applicative[F]): ContextFold[F, S, E] = _ => FoldOption.empty[F, S, E]

  /** Creates an instance of `ContextFold` from a function; similar to `FoldOption.of` */
  def of[F[_], S, E](f: (FoldContext[F], Option[S], E) => F[Option[S]]): ContextFold[F, S, E] =
    context => {
      val fold = f(context, _, _)
      FoldOption.of[F, S, E]((s, e) => fold(s, e))
    }

  /** Convenience method to transform a non-context-aware `FoldOption` into a contextual one */
  def fromFold[F[_], S, E](fold: FoldOption[F, S, E]): ContextFold[F, S, E] =
    ContextFold.of((_, s, e) => fold(s, e))
}
