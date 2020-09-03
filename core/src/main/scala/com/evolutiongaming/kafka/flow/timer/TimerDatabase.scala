package com.evolutiongaming.kafka.flow.timer

import cats.Applicative
import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.mtl.MonadState
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.sstream.Stream
import com.olegpy.meow.effects._

private[timer] trait TimerDatabase[F[_], K, T] {

  /** Adds timer into a database if it does not exist yet */
  def persist(key: K, timer: T): F[Unit]

  /** Restores timers for the key, if any */
  def get(key: K): Stream[F, T]

  /** Deletes all timers for they key, if any */
  def delete(key: K): F[Unit]

}
private[timer] object TimerDatabase {

  /** Creates in-memory database implementation. */
  def memory[F[_]: Sync, K, T]: F[TimerDatabase[F, K, T]] =
    Ref.of[F, Map[K, Set[T]]](Map.empty) map { storage =>
      memory(storage.stateInstance)
    }

  /** Creates in-memory database implementation.
    *
    * The data will survive destruction of specific `Timers` instance,
    * but will not survive destruction of specific `TimerDatabase` instance.
    */
  def memory[F[_]: Monad, K, T](
    storage: MonadState[F, Map[K, Set[T]]]
  ): TimerDatabase[F, K, T] = new TimerDatabase[F, K, T] {

    def persist(key: K, timer: T) = storage modify { storage =>
      val existingTimers = storage.getOrElse(key, Set.empty[T])
      val updatedTimers = existingTimers + timer
      storage + (key -> updatedTimers)
    }

    def get(key: K) = Stream.lift(storage.get) flatMap { storage =>
      val timers = storage get key
      Stream.from(timers.toList.flatten)
    }

    def delete(key: K) = storage modify (_ - key)

  }

  def empty[F[_]: Applicative, K, T]: TimerDatabase[F, K, T] = new TimerDatabase[F, K, T] {
    def persist(key: K, timer: T) = ().pure[F]
    def get(key: K) = Stream.empty
    def delete(key: K) = ().pure[F]
  }

  implicit class TimerDatabaseKafkaTimerOps[F[_], K](
    val self: TimerDatabase[F, K, KafkaTimer]
  ) extends AnyVal {
    def timersOf(
      implicit F: Sync[F], log: Log[F]
    ): TimersOf[F, K] = { (key, createdAt) =>
      Timestamps.of(createdAt) flatMap { implicit timestamps =>
        Timers.of(key, self) map { timers =>
          TimerContext(timers, timestamps)
        }
      }
    }
  }

}
