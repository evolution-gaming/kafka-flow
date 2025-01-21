package com.evolutiongaming.kafka.flow.key

import cats.syntax.all._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix

trait Keys[F[_]] extends KeyWriter[F]

/** Provided a persistence for a specific key */
trait KeyWriter[F[_]] {

  /** Flushes buffer to a database */
  def flush: F[Unit]

  /** Removes state from the buffers and optionally also from persistence.
    *
    * @param persist
    *   if `true` then also calls underlying database, flushes buffers only otherwise.
    */
  def delete(persist: Boolean): F[Unit]

}
object Keys {

  /** Creates a buffer for a given writer */
  @deprecated("Use `of` instead", "5.0.6")
  private[key] def apply[F[_]: Monad: Log, K](
    key: K,
    database: KeyDatabase[F, K]
  ): Keys[F] = new Keys[F] {

    def flush: F[Unit] = database.persist(key)

    def delete(persist: Boolean): F[Unit] =
      if (persist) {
        database.delete(key) *> Log[F].info("deleted key")
      } else {
        ().pure[F]
      }

  }

  private[key] def of[F[_]: Monad: Log, K: LogPrefix](
    key: K,
    database: KeyDatabase[F, K]
  ): Keys[F] = new Keys[F] {

    private val prefixedLog = Log[F].prefixed(s"[${LogPrefix[K].extract(key)}]")

    def flush: F[Unit] = database.persist(key)

    def delete(persist: Boolean): F[Unit] =
      if (persist) {
        database.delete(key) *> prefixedLog.info("deleted key")
      } else {
        ().pure[F]
      }

  }

  def empty[F[_]: Applicative]: Keys[F] = new Keys[F] {
    def flush: F[Unit]                    = ().pure[F]
    def delete(persist: Boolean): F[Unit] = ().pure[F]
  }

}
