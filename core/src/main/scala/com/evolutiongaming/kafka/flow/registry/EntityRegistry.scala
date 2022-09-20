package com.evolutiongaming.kafka.flow.registry

import cats.effect.Resource
import cats.effect.kernel.Ref
import cats.syntax.all._
import cats.{Applicative, Monad}

trait EntityRegistry[F[_], K, S] {
  def register(key: K, state: F[Option[S]]): Resource[F, Unit]

  def get(key: K): F[Option[S]]

  def getAll: F[Map[K, S]]
}

object EntityRegistry {
  def empty[F[_], K, S](implicit F: Applicative[F]): EntityRegistry[F, K, S] = new EmptyEntityRegistry[F, K, S]()

  def memory[F[_], K, S](implicit F: Monad[F], R: Ref.Make[F]): F[EntityRegistry[F, K, S]] =
    R.refOf(Map.empty[K, F[Option[S]]]).map(ref => new InMemoryEntityRegistry[F, K, S](ref))

  def const[F[_], K, S](values: Map[K, S])(implicit F: Applicative[F]): EntityRegistry[F, K, S] =
    new ConstEntityRegistry[F, K, S](values)

  final class EmptyEntityRegistry[F[_], K, S](implicit F: Applicative[F]) extends EntityRegistry[F, K, S] {
    override def register(key: K, state: F[Option[S]]): Resource[F, Unit] = Resource.unit
    override def get(key: K): F[Option[S]] = F.pure(none[S])
    override def getAll: F[Map[K, S]] = F.pure(Map.empty)
  }

  final class ConstEntityRegistry[F[_], K, S](values: Map[K, S])(implicit F: Applicative[F])
      extends EntityRegistry[F, K, S] {
    override def register(key: K, state: F[Option[S]]): Resource[F, Unit] = Resource.unit
    override def get(key: K): F[Option[S]] = F.pure(values.get(key))
    override def getAll: F[Map[K, S]] = F.pure(values)
  }

  final class InMemoryEntityRegistry[F[_], K, S](ref: Ref[F, Map[K, F[Option[S]]]])(implicit F: Monad[F])
      extends EntityRegistry[F, K, S] {
    override def register(key: K, state: F[Option[S]]): Resource[F, Unit] = {
      Resource.make(ref.update(map => map + (key -> state)))(_ => ref.update(map => map - key))
    }

    override def get(key: K): F[Option[S]] =
      ref.get.flatMap(m =>
        m.get(key) match {
          case Some(f) => f
          case None    => F.pure(None)
        }
      )

    override def getAll: F[Map[K, S]] = ref.get.flatMap { map =>
      map.toList
        .traverseFilter { case (key, readValue) => readValue.map(maybeValue => maybeValue.map(v => key -> v)) }
        .map(_.toMap)
    }
  }
}
