package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.ApplicativeError
import cats.Functor
import cats.Monad
import cats.syntax.all.*

/** Convenience methods for using `Fold` with optional state */
final case class FoldOption[F[_], S, A](value: Fold[F, Option[S], A]) {

  /** Alias for `run` */
  def apply(s: Option[S], a: A): F[Option[S]] = value(s, a)

  /** Transforms the input `A` of the `Fold` to something else.
    *
    * The common use for this method is to use `Fold` with the parsed input to construct a new `Fold` which knows how to
    * parse the binary representation.
    */
  def contramap[B](f: B => A): FoldOption[F, S, B] =
    FoldOption(value contramap f)

  /** Transforms the input `A` of the `Fold` to something else.
    *
    * Same as `contramap`, but allows to have an effect when calculating new input.
    */
  def contramapM[B](f: B => F[A])(implicit F: Monad[F]): FoldOption[F, S, B] =
    FoldOption(value contramapM f)

  /** Transforms the state `S` of the `Fold` to something else.
    *
    * It is possible to use additional information from `A` to build a new state.
    *
    * The common use for this method is to augument original state with some metainformation, i.e. offset or sequence
    * number.
    *
    * See also `StateT#transformS` for more details.
    */
  def transformState[T](f: T => S)(g: (S, A) => T)(implicit F: Functor[F]): FoldOption[F, T, A] =
    FoldOption {
      value.transformState[Option[T]](_ map f) { (s, a) =>
        s map (g(_, a))
      }
    }

  /** Effectfully transforms the state `S` of the `Fold` to something else.
    *
    * Same as `dimap`, but allows to have an effect when calculating new state.
    *
    * Useful, for example, if `A` should be parsed to get the required additional information, but parsing may fail.
    */
  def transformStateM[T](f: T => F[S])(g: (S, A) => F[T])(implicit F: Monad[F]): FoldOption[F, T, A] =
    FoldOption {
      value.transformStateM[Option[T]](_ traverse f) { (s, a) =>
        s traverse (g(_, a))
      }
    }

  /** Do `transformState` and `contramap` at the same time */
  def transform[T, B](f: T => S, g: B => A)(h: (S, B) => T)(implicit F: Functor[F]): FoldOption[F, T, B] =
    contramap(g).transformState(f)(h)

  /** Same as `transform`, but allows to have an effect */
  def transformM[T, B](f: T => F[S], g: B => F[A])(h: (S, B) => F[T])(implicit F: Monad[F]): FoldOption[F, T, B] =
    contramapM(g).transformStateM(f)(h)

  /** Applies `A` second time using another `Fold`.
    *
    * If `None` is returned by first `Fold` then the second will not be executed.
    */
  def flatMap[T](f: S => FoldOption[F, S, A])(implicit F: Monad[F]): FoldOption[F, S, A] =
    FoldOption {
      value flatMap { s =>
        (s map f getOrElse FoldOption.empty[F, S, A]).value
      }
    }

  /** Applies `A` second time using another `Fold`.
    *
    * If `None` is returned by first `Fold` then the second will not be executed.
    */
  def productR(that: FoldOption[F, S, A])(implicit F: Monad[F]): FoldOption[F, S, A] =
    this flatMap { _ => that }

  /** Alias for `productR` */
  def *>(that: FoldOption[F, S, A])(implicit F: Monad[F]): FoldOption[F, S, A] =
    productR(that)

  /** Filters incoming `A` elements by a previous `S`.
    *
    * If `S` is `None` then the filter is ignored.
    */
  def filter(f: (S, A) => Boolean)(implicit F: Applicative[F]): FoldOption[F, S, A] =
    FoldOption {
      value filter { (s, a) =>
        s map (f(_, a)) getOrElse true
      }
    }

  /** Effectfully filters incoming `A` elements by a previous `S`.
    *
    * Same as `filter`, but allows to have an effect when calculating condition.
    */
  def filterM(f: (S, A) => F[Boolean])(implicit F: Monad[F]): FoldOption[F, S, A] =
    FoldOption {
      value filterM { (s, a) =>
        s map (f(_, a)) getOrElse true.pure[F]
      }
    }

  /** Filters and transforms incoming `B` elements. */
  def contraCollect[B](f: PartialFunction[B, A])(implicit F: Applicative[F]): FoldOption[F, S, B] =
    FoldOption(value contraCollect f)

  /** Allows to gracefully handle the errror happening during flow.
    *
    * I.e. one could keep / modify the existing state or replace it with some other value.
    */
  def handleErrorWith[E](f: (S, E) => F[S])(implicit F: ApplicativeError[F, E]): FoldOption[F, S, A] =
    FoldOption {
      value.handleErrorWith[E] { (s, e) =>
        s traverse (f(_, e))
      }
    }

}
object FoldOption {

  def of[F[_], S, A](run: (Option[S], A) => F[Option[S]]): FoldOption[F, S, A] =
    FoldOption(Fold(run))

  /** Sets the state to `None` regardless of input.
    *
    * The common use is to return it in `flatMap` to set an empty state.
    */
  def empty[F[_]: Applicative, S, A]: FoldOption[F, S, A] = FoldOption {
    Fold.set(None)
  }

  /** Sets the state to `s` regardless of input */
  def set[F[_]: Applicative, S, A](s: S): FoldOption[F, S, A] = FoldOption {
    Fold.set(Some(s))
  }

  def modifyFold[F[_]: Applicative, S, A](f: Option[S] => Option[S]): FoldOption[F, S, A] =
    FoldOption(Fold.modify(f))

  def tapFold[F[_]: Monad, S, A](f: (Option[S], A) => F[Unit]): FoldOption[F, S, A] =
    FoldOption(Fold.tap(f))

}
