package com.evolutiongaming.kafka.flow

import cats.Applicative

/** Provides an access to some additional internal functionality of the framework.
  *
  * It's passed as an argument to a user-defined code of `EnhancedFold`, allowing one to use this functionality when
  * processing incoming events.
  *
  * An instance is created per key when the key is recovered (either from persistence or when encountered the first time
  * in a source Kafka topic)
  *
  * @see
  *   [[com.evolutiongaming.kafka.flow.EnhancedFold]] for the user API allowing construction of an enhanced fold
  */
trait KeyFlowExtras[F[_]] {

  /** Requests to persist a current state of the key. Calling this function doesn't guarantee that the state will be
    * persisted immediately; it persists the state after the fold is executed
    *
    * @see
    *   See [[com.evolutiongaming.kafka.flow.AdditionalStatePersist]] for the underlying implementation of persisting
    */
  def requestAdditionalPersist: F[Unit]
}

object KeyFlowExtras {
  def empty[F[_]](implicit F: Applicative[F]): KeyFlowExtras[F] = new KeyFlowExtras[F] {
    def requestAdditionalPersist: F[Unit] = F.unit
  }

  def of[F[_]](requestPersist: F[Unit]): KeyFlowExtras[F] = new KeyFlowExtras[F] {
    def requestAdditionalPersist: F[Unit] = requestPersist
  }
}
