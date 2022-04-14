package com.evolutiongaming.kafka.flow.key

import cats.{Applicative, Monad}
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import cats.mtl.MonadState
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream
import com.olegpy.meow.effects._

trait KeyDatabase[F[_], K] {

  /** Adds the key to the database if it exists */
  def persist(key: K): F[Unit]

  /** Deletes snapshot for they key, if any */
  def delete(key: K): F[Unit]

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, K]

  def keysOf(implicit F: Monad[F], logOf: LogOf[F]): F[KeysOf[F, K]] =
    logOf(KeyDatabase.getClass) map { implicit log => KeysOf(this) }

}
object KeyDatabase {

  /** Creates in-memory database implementation */
  def memory[F[_]: Sync, K]: F[KeyDatabase[F, K]] =
    Ref.of[F, Set[K]](Set.empty[K]) map { storage =>
      memory(storage.stateInstance)
    }

  /** Creates in-memory database implementation */
  def memory[F[_]: Monad, K](storage: MonadState[F, Set[K]]): KeyDatabase[F, K] =
    new KeyDatabase[F, K] {

      def persist(key: K) =
        storage modify (_ + key)

      def delete(key: K) =
        storage modify (_ - key)

      def all(applicationId: String, groupId: String, topicPartition: TopicPartition) =
        Stream.lift(storage.get) flatMap { keys =>
          Stream.from(keys.toList)
        }

    }

  def empty[F[_]: Applicative, K]: KeyDatabase[F, K] =
    new KeyDatabase[F, K] {
      def persist(key: K) = ().pure
      def delete(key: K) = ().pure
      def all(applicationId: String, groupId: String, topicPartition: TopicPartition) = Stream.empty
    }
}
