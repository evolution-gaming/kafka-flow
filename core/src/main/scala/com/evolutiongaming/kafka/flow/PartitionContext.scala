package com.evolutiongaming.kafka.flow

import cats.Applicative
import cats.Monad
import cats.effect.Resource
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.mtl.MonadState
import cats.syntax.all._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.skafka.Offset
import com.olegpy.meow.effects._

/** Partition specific metainformation inside of topic */
trait PartitionContext[F[_]] {
  def commit(offset: Offset): F[Unit]
}
object PartitionContext {

  def apply[F[_]](implicit F: PartitionContext[F]): PartitionContext[F] = F

  def empty[F[_]: Applicative]: PartitionContext[F] = new PartitionContext[F] {
    def commit(offset: Offset) = ().pure[F]
  }

  def of[F[_]: Sync: Log](removeFromCache: F[Unit]): F[KeyContext[F]] =
    Ref.of[F, Option[Offset]](None) map { storage =>
      KeyContext(storage.stateInstance, removeFromCache)
    }

  def apply[F[_]: Monad: Log](
    storage: MonadState[F, Option[Offset]],
    removeFromCache: F[Unit]
  ): KeyContext[F] = new KeyContext[F] {
    def holding = storage.get
    def hold(offset: Offset) = storage.set(Some(offset))
    def remove = storage.set(None) *> removeFromCache
    def log = Log[F]
  }

  def resource[F[_]: Sync](
    removeFromCache: F[Unit],
    log: Log[F]
  ): Resource[F, KeyContext[F]] = {
    implicit val _log = log
    Resource.liftF(of(removeFromCache))
  }

}
