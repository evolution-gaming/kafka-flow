package com.evolutiongaming.kafka.flow

import cats.Functor
import cats.mtl.Stateful
import cats.syntax.all.*
import monocle.Lens

object MonadStateHelper {

  implicit class MonadStateOps[F[_]: Functor, A, B](val self: Stateful[F, A]) {
    def focus(lens: Lens[A, B]): Stateful[F, B] = new Stateful[F, B] {
      val monad     = self.monad
      def get       = self.get.map(lens.get(_))
      def set(b: B) = self.modify(lens.replace(b))
    }
  }

}
