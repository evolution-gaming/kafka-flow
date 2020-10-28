package com.evolutiongaming.kafka.flow.timer

import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.LogOf

trait TimersOf[F[_], K] {

  def apply(key: K, createdAt: Timestamp): F[TimerContext[F]]

}

object TimersOf {

  @deprecated("use version with `LogOf` to minimze number of required implicits", "0.3.3")
  def memory[F[_]: Sync: Log, K]: TimersOf[F, K] = { (key, createdAt) =>
    Timestamps.of(createdAt) flatMap { implicit timestamps =>
      Timers.memory(key) map { timers =>
        TimerContext(timers, timestamps)
      }
    }
  }

  def memory[F[_]: Sync: LogOf, K]: F[TimersOf[F, K]] =
    LogOf[F].apply(TimersOf.getClass) map { implicit log => (key, createdAt) =>
      Timestamps.of(createdAt) flatMap { implicit timestamps =>
        Timers.memory(key) map { timers =>
          TimerContext(timers, timestamps)
        }
      }
    }

}
