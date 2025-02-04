package com.evolutiongaming.kafka.flow.effect

import cats.Monad
import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.functor.*

/** meow-mtl library is not updated and supported anymore, this is just a straight copypasta from there */
private[flow] object CatsEffectMtlInstances {
  implicit class RefEffects[F[_], A](val self: Ref[F, A]) extends AnyVal {
    def stateInstance(implicit F: Monad[F]): Stateful[F, A] = new RefStateful[F, A](self)
  }

  class RefStateful[F[_], S](ref: Ref[F, S])(implicit F: Monad[F]) extends Stateful[F, S] {
    val monad: Monad[F]                      = F
    def get: F[S]                            = ref.get
    def set(s: S): F[Unit]                   = ref.set(s)
    override def inspect[A](f: S => A): F[A] = ref.get.map(f)
    override def modify(f: S => S): F[Unit]  = ref.update(f)
  }
}
