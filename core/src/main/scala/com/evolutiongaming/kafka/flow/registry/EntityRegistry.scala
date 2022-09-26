package com.evolutiongaming.kafka.flow.registry

import cats.effect.concurrent.Ref
import cats.effect.{Resource, Sync}
import cats.syntax.all._
import cats.{Applicative, Monad}

/** Observability API allowing to inspect current state of in-memory entities externally (from HTTP handler, for example).
  * When passed to the library, it's used to register an association between entity key and the function
  * to obtain its current state when the state is initialized for the first time.
  * When the entity is removed from the in-memory cache, this association is also supposed to be removed.
  *
  * There are three pre-defined implementations currently provided:
  *   - `EntityRegistry.empty` for no-op implementation
  *   - `EntityRegistry.const` for immutable constant data
  *   - `EntityRegistry.memory`, fully functional in-memory registry
  * @tparam K entity key
  * @tparam S entity state
  */
trait EntityRegistry[F[_], K, S] {

  /** Registers an association between an entity's key and the function to obtain current state of it.
    * This is used internally by the library and shouldn't be called by the library users.
    * @param key entity key
    * @param state computation (effectively function) to obtain the current value of the entity
    * @return `Resource` which acquisition registers the entity and releasing removes it from the registry
    */
  def register(key: K, state: F[Option[S]]): Resource[F, Unit]

  /** Returns current state of the entity by key (or None if there's no such entity or it has empty state)
    * @param key entity key
    * @return `Some` when the entity is present and has non-empty state, None otherwise
    */
  def get(key: K): F[Option[S]]

  /** Returns keys and states of all currently registered in-memory entities.
    * Filters out entities having None as state
    */
  def getAll: F[Map[K, S]]
}

object EntityRegistry {
  def empty[F[_], K, S](implicit F: Applicative[F]): EntityRegistry[F, K, S] = new EmptyEntityRegistry[F, K, S]()

  def memory[F[_], K, S](implicit F: Sync[F]): F[EntityRegistry[F, K, S]] =
    Ref.of(Map.empty[K, F[Option[S]]]).map(ref => new InMemoryEntityRegistry[F, K, S](ref))

  def const[F[_], K, S](values: Map[K, S])(implicit F: Applicative[F]): EntityRegistry[F, K, S] =
    new ConstEntityRegistry[F, K, S](values)

  /** No-op registry, always returning None and an empty map on `get` and no-op on `register` */
  final class EmptyEntityRegistry[F[_], K, S](implicit F: Applicative[F]) extends EntityRegistry[F, K, S] {
    override def register(key: K, state: F[Option[S]]): Resource[F, Unit] = Resource.pure(())
    override def get(key: K): F[Option[S]] = F.pure(none[S])
    override def getAll: F[Map[K, S]] = F.pure(Map.empty)
  }

  /** Immutable registry with a no-op `register` method. `get` and `getAll` query the passed map */
  final class ConstEntityRegistry[F[_], K, S](values: Map[K, S])(implicit F: Applicative[F])
      extends EntityRegistry[F, K, S] {
    override def register(key: K, state: F[Option[S]]): Resource[F, Unit] = Resource.pure(())
    override def get(key: K): F[Option[S]] = F.pure(values.get(key))
    override def getAll: F[Map[K, S]] = F.pure(values)
  }

  /** Mutable registry that keeps the state in a `Ref` */
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
