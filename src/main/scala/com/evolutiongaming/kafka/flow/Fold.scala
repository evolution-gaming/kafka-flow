package com.evolutiongaming.kafka.flow

trait Fold[F[_], S, E] {

  def apply(state: Option[S], event: E): F[Option[S]]
}