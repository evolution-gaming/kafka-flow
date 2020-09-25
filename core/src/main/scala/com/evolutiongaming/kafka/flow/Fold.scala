package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.ApplicativeError
import cats.Functor
import cats.Monad
import cats.syntax.all._

/** Reads a state and effectfully produces a new one.
  *
  * Roughly speaking it is `Kleisli[F, (S, A), S]` with the main additional
  * requirement that input and output types are not independent (because `S`
  * is present both in input and output values).
  */
final case class Fold[F[_], S, A](run: (S, A) => F[S]) {

  /** Alias for `run` */
  def apply(s: S, a: A): F[S] = run(s, a)

  /** Transforms the input `A` of the `Fold` to something else.
    *
    * The common use for this method is to use `Fold` with the parsed
    * input to construct a new `Fold` which knows how to parse the
    * binary representation.
    */
  def contramap[B](f: B => A): Fold[F, S, B] =
    Fold { (s, a) =>
      run(s, f(a))
    }

  /** Transforms the input `A` of the `Fold` to something else.
    *
    * Same as `contramap`, but allows to have an effect when calculating new input.
    */
  def contramapM[B](f: B => F[A])(implicit F: Monad[F]): Fold[F, S, B] =
    Fold { (s, a) =>
      f(a) flatMap (run(s, _))
    }

  /** Transforms the state `S` of the `Fold` to something else.
    *
    * It is possible to use additional information from `A` to build a new state.
    *
    * The common use for this method is to augument original state with
    * some metainformation, i.e. offset or sequence number.
    *
    * See also `StateT#transformS` for more details.
    */
  def transformState[T](f: T => S)(g: (S, A) => T)(implicit F: Functor[F]): Fold[F, T, A] =
    Fold { (t, a) =>
      run(f(t), a) map { s =>
        g(s, a)
      }
    }

  /** Effectfully transforms the state `S` of the `Fold` to something else.
    *
    * Same as `transformState`, but allows to have an effect when calculating new state.
    *
    * Useful, for example, if `A` should be parsed to get the required
    * additional information, but parsing may fail.
    */
  def transformStateM[T](f: T => F[S])(g: (S, A) => F[T])(implicit F: Monad[F]): Fold[F, T, A] =
    Fold { (t, a) =>
      for {
        s <- f(t)
        s <- run(s, a)
        t <- g(s, a)
      } yield t
    }

  /** Do `transformState` and `contramap` at the same time */
  def transform[T, B](f: T => S, g: B => A)(h: (S, B) => T)(implicit F: Functor[F]): Fold[F, T, B] =
    contramap(g).transformState(f)(h)

  /** Same as `transform`, but allows to have an effect */
  def transformM[T, B](f: T => F[S], g: B => F[A])(h: (S, B) => F[T])(implicit F: Monad[F]): Fold[F, T, B] =
    contramapM(g).transformStateM(f)(h)

  /** Applies `A` second time using another `Fold` */
  def flatMap(f: S => Fold[F, S, A])(implicit F: Monad[F]): Fold[F, S, A] =
    Fold { (s, a) =>
      run(s, a) flatMap { s =>
        f(s).run(s, a)
      }
    }

  /** Applies `A` second time using another `Fold` */
  def productR(that: Fold[F, S, A])(implicit F: Monad[F]): Fold[F, S, A] =
    this flatMap { _ => that }

  /** Alias for `productR` */
  def *>(that: Fold[F, S, A])(implicit F: Monad[F]): Fold[F, S, A] =
    productR(that)

  /** Filters incoming `A` elements by a previous `S` */
  def filter(f: (S, A) => Boolean)(implicit F: Applicative[F]): Fold[F, S, A] =
    Fold { (s, a) =>
      if (f(s, a)) run(s, a) else s.pure[F]
    }

  /** Effectfully filters incoming `A` elements by a previous `S`.
    *
    * Same as `filter`, but allows to have an effect when calculating condition.
    */
  def filterM(f: (S, A) => F[Boolean])(implicit F: Monad[F]): Fold[F, S, A] =
    Fold { (s, a) =>
      f(s, a).ifM(run(s, a), s.pure[F])
    }

  /** Filters and transforms incoming `B` elements */
  def contraCollect[B](f: PartialFunction[B, A])(implicit F: Applicative[F]): Fold[F, S, B] =
    Fold { (s, b) =>
      val a = f.unapply(b)
      a traverse (run(s, _)) map (_ getOrElse s)
    }

  /** Allows to gracefully handle the errror happening during flow.
    *
    * I.e. one could keep / modify the existing state or replace it with some other value.
    */
  def handleErrorWith[E](f: (S, E) => F[S])(implicit F: ApplicativeError[F, E]): Fold[F, S, A] =
    Fold { (s, a) =>
      run(s, a) handleErrorWith (f(s, _))
    }

}

object Fold {

  def set[F[_]: Applicative, S, A](s: S): Fold[F, S, A] =
    Fold { (_, _) => s.pure[F] }

  def modify[F[_]: Applicative, S, A](f: S => S): Fold[F, S, A] =
    Fold { (s, _) => f(s).pure[F] }

  def tap[F[_]: Monad, S, A](f: (S, A) => F[Unit]): Fold[F, S, A] =
    Fold { (s, a) => f(s, a) as s }

}