package com.evolutiongaming.kafka.flow.key

import cats.effect.Sync
import cats.syntax.all._
import cats.Monad
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.TopicPartition
import com.evolutiongaming.sstream.Stream

trait KeysOf[F[_], K] {

  def apply(key: K): Keys[F]

  def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, K]

}
object KeysOf {

  def memory[F[_]: Sync: Log, K]: F[KeysOf[F, K]] =
    KeyDatabase.memory[F, K] map { database =>
      KeysOf(database)
    }

  /** Creates `KeysOf` with a passed logger */
  def apply[F[_]: Monad: Log, K](
    database: KeyDatabase[F, K]
  ): KeysOf[F, K] = new KeysOf[F, K] {
    def apply(key: K) = Keys(key, database)
    def all(applicationId: String, groupId: String, topicPartition: TopicPartition) =
      database.all(applicationId, groupId, topicPartition)
  }

}
