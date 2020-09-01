package com.evolutiongaming.kafka.flow.timer

import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.catshelper.Log

trait TimersOf[F[_], K] {

  def apply(key: K, createdAt: Timestamp): F[TimerContext[F]]

}

object TimersOf {

  def memory[F[_]: Sync: Log, K]: TimersOf[F, K] = { (key, createdAt) =>
    Timestamps.of(createdAt) flatMap { implicit timestamps =>
      Timers.memory(key) map { timers =>
        TimerContext(timers, timestamps)
      }
    }
  }

}
