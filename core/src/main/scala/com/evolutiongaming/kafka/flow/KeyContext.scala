package com.evolutiongaming.kafka.flow

import cats.effect.{Ref, Resource}
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.{Log, LogOf}
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
  def log(source: Class[_]): F[Log[F]]
}
object KeyContext {

  def apply[F[_]](implicit F: KeyContext[F]): KeyContext[F] = F

  def empty[F[_]: Applicative]: KeyContext[F] = new KeyContext[F] {
    def holding               = none[Offset].pure[F]
    def hold(offset: Offset)  = ().pure[F]
    def remove                = ().pure[F]
    def log(source: Class[_]) = Log.empty[F].pure[F]
  }

  def of[F[_]: Ref.Make: Monad: LogOf](removeFromCache: F[Unit], mdc: Log.Mdc): F[KeyContext[F]] =
    Ref.of[F, Option[Offset]](None) map { storage =>
      KeyContext(storage.stateInstance, removeFromCache, mdc)
    }

  def apply[F[_]: Monad: LogOf](
    storage: Stateful[F, Option[Offset]],
    removeFromCache: F[Unit],
    mdc: Log.Mdc
  ): KeyContext[F] = new KeyContext[F] {
    def holding               = storage.get
    def hold(offset: Offset)  = storage.set(Some(offset))
    def remove                = storage.set(None) *> removeFromCache
    def log(source: Class[_]) = LogOf[F].apply(source).map(_.withMdc(mdc))
  }

  def resource[F[_]: Ref.Make: Monad: LogOf](
    removeFromCache: F[Unit],
    mdc: Log.Mdc
  ): Resource[F, KeyContext[F]] =
    Resource.eval(of(removeFromCache, mdc))

}
