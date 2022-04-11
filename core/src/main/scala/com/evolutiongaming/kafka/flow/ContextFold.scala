package com.evolutiongaming.kafka.flow

import cats.syntax.all._
import cats.{Applicative, ApplicativeError, Monad}

/** Given an optional state `S` and an incoming event `E`, produces a resulting optional state `S`.
  * This is the main user API to define stateful processing.
  * In contrast to `FoldOption`, it also accepts `FoldContext`, providing an access to some additional framework APIs.
  *
  * This abstraction is not expresses in terms of `FoldOption` to avoid unnecessary allocations of `FoldOption`
  * when the logic involves multiple nested `ContextFold` instances invoking each other.
  *
  * @tparam F computation effect
  * @tparam S state
  * @tparam E incoming event
  */
trait ContextFold[F[_], S, E] {

  def apply(context: FoldContext[F], s: Option[S], e: E): F[Option[S]]

  final def flatMap(f: S => ContextFold[F, S, E])(implicit F: Monad[F]): ContextFold[F, S, E] =
    (context, state0, event) => {
      apply(context, state0, event).flatMap {
        case s @ Some(state1) => f(state1).apply(context, s, event)
        case None             => F.pure(None)
      }
    }

  final def filter(f: (S, E) => Boolean)(implicit F: Applicative[F]): ContextFold[F, S, E] =
    (context, state0, event) =>
      state0 match {
        case Some(state) => if (f(state, event)) apply(context, state0, event) else F.pure(state0)
        case None        => apply(context, state0, event)
      }

  final def filterM(f: (S, E) => F[Boolean])(implicit F: Monad[F]): ContextFold[F, S, E] =
    (context, state0, event) =>
      state0 match {
        case Some(state) => F.ifM(f(state, event))(ifTrue = apply(context, state0, event), ifFalse = F.pure(state0))
        case None        => apply(context, state0, event)
      }

  final def handleErrorWith[E1](f: (S, E1) => F[S])(implicit F: ApplicativeError[F, E1]): ContextFold[F, S, E] =
    (context, state0, event) =>
      apply(context, state0, event).handleErrorWith(e => state0.traverse(state => f(state, e)))

}

object ContextFold {

  def empty[F[_], S, E](implicit F: Applicative[F]): ContextFold[F, S, E] = (_, _, _) => F.pure(None)

  /** Creates an instance of `ContextFold` from a function; similar to `FoldOption.of` */
  def of[F[_], S, E](f: (FoldContext[F], Option[S], E) => F[Option[S]]): ContextFold[F, S, E] =
    (context, state, event) => f(context, state, event)

  /** Convenience method to transform a non-context-aware `FoldOption` into a contextual one */
  def fromFold[F[_], S, E](fold: FoldOption[F, S, E]): ContextFold[F, S, E] =
    ContextFold.of((_, s, e) => fold(s, e))
}
