package com.evolutiongaming.kafka.flow

import cats.Functor
import cats.mtl.Stateful
import cats.syntax.all._
import monocle.Lens

object MonadStateHelper {

  implicit class MonadStateOps[F[_]: Functor, A, B](val self: Stateful[F, A]) {
    def focus(lens: Lens[A, B]): Stateful[F, B] = new Stateful[F, B] {
      val monad = self.monad
      def get = self.get map lens.get
      def set(b: B) = self modify lens.set(b)
    }
  }

}
