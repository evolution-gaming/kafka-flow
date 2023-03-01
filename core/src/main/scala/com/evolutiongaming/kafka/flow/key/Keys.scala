package com.evolutiongaming.kafka.flow.key

import cats.Applicative
import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log

trait Keys[F[_]] extends KeyWriter[F]

/** Provided a persistence for a specific key */
trait KeyWriter[F[_]] {

  /** Flushes buffer to a database */
  def flush: F[Unit]

  /** Removes state from the buffers and optionally also from persistence.
    *
    * @param persist if `true` then also calls underlying database, flushes
    * buffers only otherwise.
    */
  def delete(persist: Boolean): F[Unit]

}
object Keys {

  /** Creates a buffer for a given writer */
  private[key] def apply[F[_]: Monad: Log, K](
    key: K,
    database: KeyDatabase[F, K]
  ): Keys[F] = new Keys[F] {

    def flush: F[Unit] = database.persist(key)

    def delete(persist: Boolean): F[Unit] =
      if (persist) {
        database.delete(key) *>
          Log[F].info("deleted key")
      } else {
        ().pure[F]
      }

  }

  def empty[F[_]: Applicative]: Keys[F] = new Keys[F] {
    def flush: F[Unit]                    = ().pure[F]
    def delete(persist: Boolean): F[Unit] = ().pure[F]
  }

}
