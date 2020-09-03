package com.evolutiongaming.kafka.flow.journal

import cats.effect.Sync
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.kafka.ToOffset

trait JournalsOf[F[_], K, E] {

  def apply(key: K): F[Journals[F, E]]

}
object JournalsOf {

  def memory[F[_]: Sync: Log, K, E: ToOffset]: F[JournalsOf[F, K, E]] =
    JournalDatabase.memory[F, K, E] map { database => key =>
      Journals.of(key, database)
    }

}