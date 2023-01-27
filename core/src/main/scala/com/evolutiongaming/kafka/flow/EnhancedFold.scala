package com.evolutiongaming.kafka.flow

import cats.syntax.all._
import cats.{Applicative, ApplicativeError, Monad}

/** Given an optional state `S` and an incoming event `E`, produces a resulting optional state `S`.
  * This is the main user API to define stateful processing.
  * In contrast to `FoldOption`, it also accepts `KeyFlowExtras`, providing an access to some additional framework APIs.
  *
  * This abstraction is not expressed in terms of `FoldOption` to avoid unnecessary allocations of `FoldOption`
  * when the logic involves multiple nested `EnhancedFold` instances invoking each other.
  *
  * @tparam F computation effect
  * @tparam S state
  * @tparam E incoming event
  */
trait EnhancedFold[F[_], S, E] {

  def apply(extras: KeyFlowExtras[F], s: Option[S], e: E): F[Option[S]]

  final def flatMap(f: S => EnhancedFold[F, S, E])(implicit F: Monad[F]): EnhancedFold[F, S, E] =
    (extras, state0, event) => {
      apply(extras, state0, event).flatMap {
        case s @ Some(state1) => f(state1).apply(extras, s, event)
        case None             => F.pure(None)
      }
    }

  final def handleErrorWith[E1](f: (S, E1) => F[S])(implicit F: ApplicativeError[F, E1]): EnhancedFold[F, S, E] =
    (extras, state0, event) => apply(extras, state0, event).handleErrorWith(e => state0.traverse(state => f(state, e)))

}

object EnhancedFold {

  def empty[F[_], S, E](implicit F: Applicative[F]): EnhancedFold[F, S, E] = (_, _, _) => F.pure(None)

  /** Creates an instance of `EnhancedFold` from a function; similar to `FoldOption.of` */
  def of[F[_], S, E](f: (KeyFlowExtras[F], Option[S], E) => F[Option[S]]): EnhancedFold[F, S, E] =
    (extras, state, event) => f(extras, state, event)

  /** Convenience method to transform a non-enhanced `FoldOption` into an enhanced one */
  def fromFold[F[_], S, E](fold: FoldOption[F, S, E]): EnhancedFold[F, S, E] =
    EnhancedFold.of((_, s, e) => fold(s, e))
}
