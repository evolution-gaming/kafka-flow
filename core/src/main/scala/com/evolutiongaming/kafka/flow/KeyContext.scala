package com.evolutiongaming.kafka.flow

import cats.effect.{Ref, Resource}
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.skafka.Offset

/** Key specific metainformation inside of parititon.
  *
  * Recreated each time `KeyState` is created or loaded from the storage.
  */
trait KeyContext[F[_]] {
  def holding: F[Option[Offset]]
  def hold(offset: Offset): F[Unit]
  def remove: F[Unit]
  def log: Log[F]
  def key: String
}
object KeyContext {

  def apply[F[_]](implicit F: KeyContext[F]): KeyContext[F] = F

  def empty[F[_]: Applicative]: KeyContext[F] = new KeyContext[F] {
    def log                  = Log.empty
    def holding              = none[Offset].pure[F]
    def hold(offset: Offset) = ().pure[F]
    def remove               = ().pure[F]
    val key                  = ""
  }

  def of[F[_]: Ref.Make: Monad: Log](removeFromCache: F[Unit], key: String): F[KeyContext[F]] =
    Ref.of[F, Option[Offset]](None) map { storage =>
      KeyContext(storage.stateInstance, removeFromCache, key)
    }

  def apply[F[_]: Monad: Log](
    storage: Stateful[F, Option[Offset]],
    removeFromCache: F[Unit],
    _key: String
  ): KeyContext[F] = new KeyContext[F] {
    def holding              = storage.get
    def hold(offset: Offset) = storage.set(Some(offset))
    def remove               = storage.set(None) *> removeFromCache
    def log                  = Log[F]
    val key                  = _key
  }

  def resource[F[_]: Ref.Make: Monad](
    removeFromCache: F[Unit],
    log: Log[F],
    key: String
  ): Resource[F, KeyContext[F]] = {
    implicit val _log = log
    Resource.eval(of(removeFromCache, key))
  }

}
