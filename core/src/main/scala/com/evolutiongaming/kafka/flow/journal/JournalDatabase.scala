package com.evolutiongaming.kafka.flow.journal

import cats.effect.{Ref, Sync}
import cats.mtl.Stateful
import cats.syntax.all._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._
import com.evolutiongaming.kafka.flow.kafka.ToOffset
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.sstream.Stream

import scala.collection.immutable.SortedMap

trait JournalDatabase[F[_], K, R] {

  /** Adds or replaces the event in a database */
  def persist(key: K, event: R): F[Unit]

  /** Restores journal for the key, if any */
  def get(key: K): Stream[F, R]

  /** Deletes journal for they key, if any */
  def delete(key: K): F[Unit]

  def journalsOf(implicit F: Sync[F], logOf: LogOf[F]): F[JournalsOf[F, K, R]] =
    logOf(JournalDatabase.getClass) map { implicit log => key =>
      Journals.of(key, this)
    }

}
object JournalDatabase {

  /** Creates in-memory database implementation.
    *
    * The data will survive destruction of specific `Journals` instance,
    * but will not survive destruction of specific `JournalDatabase` instance.
    */
  def memory[F[_]: Monad: Ref.Make, K, E](implicit E: ToOffset[E]): F[JournalDatabase[F, K, E]] =
    Ref.of[F, Map[K, SortedMap[Offset, E]]](Map.empty) map { storage =>
      memory(storage.stateInstance)
    }

  /** Creates in-memory database implementation.
    *
    * The data will survive destruction of specific `Journals` instance,
    * but will not survive destruction of specific `JournalDatabase` instance.
    */
  def memory[F[_]: Monad, K, E](
    storage: Stateful[F, Map[K, SortedMap[Offset, E]]]
  )(implicit E: ToOffset[E]): JournalDatabase[F, K, E] =
    new JournalDatabase[F, K, E] {

      def persist(key: K, event: E) = storage modify { storage =>
        val existingEvents = storage.getOrElse(key, SortedMap.empty[Offset, E])
        val updatedEvents  = existingEvents + (E.offset(event) -> event)
        storage + (key -> updatedEvents)
      }

      def get(key: K) = Stream.lift(storage.get) flatMap { storage =>
        val events = storage get key map (_.values.toList) getOrElse Nil
        Stream.from(events)
      }

      def delete(key: K) = storage modify (_ - key)

    }

  def empty[F[_]: Applicative, K, R]: JournalDatabase[F, K, R] =
    new JournalDatabase[F, K, R] {
      def persist(key: K, event: R) = ().pure
      def get(key: K)               = Stream.empty
      def delete(key: K)            = ().pure
    }

}
