package com.evolutiongaming.kafka.flow.timer

import cats.Monad
import cats.effect.Ref

trait TimersOf[F[_], K] {

  def apply(key: K, createdAt: Timestamp): F[TimerContext[F]]

}

object TimersOf {

  def memory[F[_]: Monad: Ref.Make, K]: F[TimersOf[F, K]] = {
    val timersOf = new TimersOf[F, K] {
      override def apply(key: K, createdAt: Timestamp): F[TimerContext[F]] = {
        TimerContext.memory[F](createdAt)
      }
    }

    Monad[F].pure(timersOf)
  }
}
