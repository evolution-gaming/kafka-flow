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
  @deprecated("Use 'run'", since = "2.2.0")
  def apply(extras: KeyFlowExtras[F], s: Option[S], e: E): F[Option[S]] = run(extras, s, e)

  def run(extras: KeyFlowExtras[F], state: Option[S], event: E): F[Option[S]]

  final def flatMap(f: S => EnhancedFold[F, S, E])(implicit F: Monad[F]): EnhancedFold[F, S, E] =
    (extras, state0, event) => {
      run(extras, state0, event).flatMap {
        case s @ Some(state1) => f(state1).run(extras, s, event)
        case None             => F.pure(None)
      }
    }

  final def filter(f: (S, E) => Boolean)(implicit F: Applicative[F]): EnhancedFold[F, S, E] =
    (extras, state0, event) =>
      state0 match {
        case Some(state) => if (f(state, event)) run(extras, state0, event) else F.pure(state0)
        case None        => run(extras, state0, event)
      }

  final def filterM(f: (S, E) => F[Boolean])(implicit F: Monad[F]): EnhancedFold[F, S, E] =
    (extras, state0, event) =>
      state0 match {
        case Some(state) => F.ifM(f(state, event))(ifTrue = run(extras, state0, event), ifFalse = F.pure(state0))
        case None        => run(extras, state0, event)
      }

  final def handleErrorWith[E1](f: (S, E1) => F[S])(implicit F: ApplicativeError[F, E1]): EnhancedFold[F, S, E] =
    (extras, state0, event) => run(extras, state0, event).handleErrorWith(e => state0.traverse(state => f(state, e)))

}

object EnhancedFold {

  def empty[F[_], S, E](implicit F: Applicative[F]): EnhancedFold[F, S, E] = new EmptyFold[F, S, E]()

  /** Creates an instance of `EnhancedFold` from a function; similar to `FoldOption.of` */
  def of[F[_], S, E](f: (KeyFlowExtras[F], Option[S], E) => F[Option[S]]): EnhancedFold[F, S, E] =
    new FunctionFold[F, S, E](f)

  /** Convenience method to transform a non-enhanced `FoldOption` into an enhanced one */
  def fromFold[F[_], S, E](fold: FoldOption[F, S, E]): EnhancedFold[F, S, E] = of((_, s, e) => fold.run(s, e))

  private final class EmptyFold[F[_], S, E](implicit F: Applicative[F]) extends EnhancedFold[F, S, E] {
    override def run(extras: KeyFlowExtras[F], state: Option[S], event: E): F[Option[S]] = F.pure(None)
  }

  private final class FunctionFold[F[_], S, E](f: (KeyFlowExtras[F], Option[S], E) => F[Option[S]])
      extends EnhancedFold[F, S, E] {
    override def run(extras: KeyFlowExtras[F], state: Option[S], event: E): F[Option[S]] = f(extras, state, event)
  }
}
