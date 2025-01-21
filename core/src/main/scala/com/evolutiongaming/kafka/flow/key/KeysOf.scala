package com.evolutiongaming.kafka.flow.key

import cats.Monad
import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream

trait KeysOf[F[_], K] {

  def apply(key: K): Keys[F]

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, K]

}
object KeysOf {

  @deprecated("Use another `memory1` instead", "5.0.6")
  def memory[F[_]: Sync: Log, K]: F[KeysOf[F, K]] =
    KeyDatabase.memory[F, K].map(database => KeysOf.apply(database))

  def memory1[F[_]: Sync: Log, K: LogPrefix]: F[KeysOf[F, K]] =
    KeyDatabase.memory[F, K].map(database => KeysOf.of(database))

  /** Creates `KeysOf` with a passed logger */
  @deprecated("Use `of` instead", "5.0.6")
  def apply[F[_]: Monad: Log, K](
    database: KeyDatabase[F, K]
  ): KeysOf[F, K] = new KeysOf[F, K] {
    def apply(key: K) = Keys(key, database)
    def all(applicationId: String, groupId: String, topicPartition: TopicPartition) =
      database.all(applicationId, groupId, topicPartition)
  }

  /** Creates `KeysOf` with a passed logger */
  def of[F[_]: Monad: Log, K: LogPrefix](
    database: KeyDatabase[F, K]
  ): KeysOf[F, K] = new KeysOf[F, K] {
    def apply(key: K) = Keys.of(key, database)
    def all(applicationId: String, groupId: String, topicPartition: TopicPartition) =
      database.all(applicationId, groupId, topicPartition)
  }

}
