package com.evolutiongaming.kafka.flow

import cats.effect.{Ref, Resource}
import cats.mtl.Stateful
import cats.syntax.all._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._
import com.evolutiongaming.skafka.Offset

/** Key specific metainformation inside of parititon.
  *
  * Recreated each time `KeyState` is created or loaded
  * from the storage.
  */
trait KeyContext[F[_]] {
  def holding: F[Option[Offset]]
  def hold(offset: Offset): F[Unit]
  def remove: F[Unit]
  def log: Log[F]
}
object KeyContext {

  def apply[F[_]](implicit F: KeyContext[F]): KeyContext[F] = F

  def empty[F[_]: Applicative]: KeyContext[F] = new KeyContext[F] {
    def log                  = Log.empty
    def holding              = none[Offset].pure[F]
    def hold(offset: Offset) = ().pure[F]
    def remove               = ().pure[F]
  }

  def of[F[_]: Ref.Make: Monad: Log](removeFromCache: F[Unit]): F[KeyContext[F]] =
    Ref.of[F, Option[Offset]](None) map { storage =>
      KeyContext(storage.stateInstance, removeFromCache)
    }

  def apply[F[_]: Monad: Log](
    storage: Stateful[F, Option[Offset]],
    removeFromCache: F[Unit]
  ): KeyContext[F] = new KeyContext[F] {
    def holding              = storage.get
    def hold(offset: Offset) = storage.set(Some(offset))
    def remove               = storage.set(None) *> removeFromCache
    def log                  = Log[F]
  }

  def resource[F[_]: Ref.Make: Monad](
    removeFromCache: F[Unit],
    log: Log[F]
  ): Resource[F, KeyContext[F]] = {
    implicit val _log = log
    Resource.eval(of(removeFromCache))
  }

}
