package com.evolutiongaming.kafka.flow.journal

import cats.{Applicative, Monad}
import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._
import com.evolutiongaming.sstream.Stream

trait Journals[F[_], E] extends JournalReader[F, E] with JournalWriter[F, E]

/** Allows to read a previously saved journal */
trait JournalReader[F[_], E] {

  /** Restores a journal */
  def read: Stream[F, E]

}

/** Provides a persistence for a specific key */
trait JournalWriter[F[_], E] {

  /** Saves the next event to a buffer.
    *
    * Note, that completing the append does not guarantee that the state will be persisted. I.e. persistence might
    * choose to do the updates in batches.
    */
  def append(event: E): F[Unit]

  /** Flush buffer to a database */
  def flush: F[Unit]

  /** Removes state from the buffers and optionally also from persistence.
    *
    * @param persist
    *   if `true` then also calls underlying database, only clears buffers otherwise.
    */
  def delete(persist: Boolean): F[Unit]

}
object Journals {

  /** Creates a buffer for a given database */
  private[journal] def of[F[_]: Monad: Ref.Make: Log, K, E](
    key: K,
    database: JournalDatabase[F, K, E]
  ): F[Journals[F, E]] =
    Ref.of[F, List[E]](List.empty) map { buffer =>
      Journals(key, database, buffer.stateInstance)
    }

  private[journal] def apply[F[_]: Monad: Log, K, E](
    key: K,
    database: JournalDatabase[F, K, E],
    buffer: Stateful[F, List[E]],
  ): Journals[F, E] = new Journals[F, E] {

    def read = database.get(key)

    def append(event: E) = buffer modify (event :: _)

    def flush = for {
      events <- buffer.get
      _ <- events.reverse traverse_ { event =>
        database.persist(key, event)
      }
    } yield ()

    def delete(persist: Boolean) = {
      val delete = if (persist) {
        database.delete(key) *>
          Log[F].info("deleted journal")
      } else {
        ().pure[F]
      }
      buffer.set(Nil) *> delete
    }

  }

  def empty[F[_]: Applicative, E]: Journals[F, E] =
    new Journals[F, E] {
      def read                     = Stream.empty[F, E]
      def append(event: E)         = ().pure[F]
      def flush                    = ().pure[F]
      def delete(persist: Boolean) = ().pure[F]
    }

}
