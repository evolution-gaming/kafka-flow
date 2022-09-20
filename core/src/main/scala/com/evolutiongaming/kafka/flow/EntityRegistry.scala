package com.evolutiongaming.kafka.flow

import cats.{Applicative, Monad}
import cats.effect.Resource
import cats.effect.kernel.Ref
import cats.syntax.all._

trait EntityRegistry[F[_], K, S] {
  def register(key: K, state: F[Option[S]]): Resource[F, Unit]

  def get(key: K): F[Option[S]]

  def getAll: F[Map[K, Option[S]]]
}

object EntityRegistry {
  def empty[F[_], K, S](implicit F: Applicative[F]): EntityRegistry[F, K, S] = new EmptyRegistry[F, K, S]()
}

class EntityRegistryImpl[F[_], K, S](ref: Ref[F, Map[K, F[Option[S]]]])(implicit F: Monad[F])
    extends EntityRegistry[F, K, S] {
  def register(key: K, state: F[Option[S]]): Resource[F, Unit] = {
    Resource.make(ref.update(_ + (key -> state)))(_ => ref.update(m => m - key))
  }

  def get(key: K): F[Option[S]] =
    ref.get.flatMap(m =>
      m.get(key) match {
        case Some(f) => f
        case None    => F.pure(None)
      }
    )

  def getAll: F[Map[K, Option[S]]] = ref.get.flatMap { map =>
    map.toList
      .traverse { case (key, f) => f.map(value => key -> value) }
      .map(_.toMap)
  }
}

class EmptyRegistry[F[_], K, S](implicit F: Applicative[F]) extends EntityRegistry[F, K, S] {
  override def register(key: K, state: F[Option[S]]): Resource[F, Unit] = Resource.unit
  override def get(key: K): F[Option[S]] = F.pure(none[S])
  override def getAll: F[Map[K, Option[S]]] = F.pure(Map.empty)
}
