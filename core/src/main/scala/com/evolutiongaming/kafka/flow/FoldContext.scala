package com.evolutiongaming.kafka.flow

import cats.Applicative

/** A context, providing an access to some additional internal functionality of the framework.
  *
  * It's passed as an argument to a user-defined code of `ContextFold`, allowing one to use this functionality when
  * processing incoming events.
  *
  * This context is created per key when the key is recovered (either from persistence or when encountered the first
  * time in a source Kafka topic)
  *
  * @see [[com.evolutiongaming.kafka.flow.ContextFold]] for the user API allowing construction of a contextual fold
  */
trait FoldContext[F[_]] {

  /** Requests to persist a current state of the key. Calling this function doesn't guarantee that the state will be
    * persisted immediately; it's up to the underlying implementation when and how it will be done
    *
    * @see See [[com.evolutiongaming.kafka.flow.AdditionalStatePersist]] for the underlying implementation of persisting
    */
  def requestAdditionalPersist: F[Unit]
}

object FoldContext {
  def empty[F[_]](implicit F: Applicative[F]): FoldContext[F] = new FoldContext[F] {
    def requestAdditionalPersist: F[Unit] = F.unit
  }

  def of[F[_]](requestPersist: F[Unit]): FoldContext[F] = new FoldContext[F] {
    def requestAdditionalPersist: F[Unit] = requestPersist
  }
}
