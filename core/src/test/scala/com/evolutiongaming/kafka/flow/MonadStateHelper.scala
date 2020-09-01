package com.evolutiongaming.kafka.flow

import cats.Functor
import cats.implicits._
import cats.mtl.DefaultMonadState
import cats.mtl.MonadState
import monocle.Lens

object MonadStateHelper {

  implicit class MonadStateOps[F[_]: Functor, A, B](val self: MonadState[F, A]) {
    def focus(lens: Lens[A, B]): MonadState[F, B] = new DefaultMonadState[F, B] {
      val monad = self.monad
      def get = self.get map lens.get
      def set(b: B) = self modify lens.set(b)
    }
  }

}