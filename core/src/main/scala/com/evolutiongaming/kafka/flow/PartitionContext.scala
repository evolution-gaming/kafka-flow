package com.evolutiongaming.kafka.flow

import cats.effect.{Ref, Resource, Sync}
import cats.syntax.all._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._
import com.evolutiongaming.skafka.Offset

/** Partition specific metainformation inside of topic */
trait PartitionContext[F[_]] {

  /** Request a commit to partition */
  def scheduleCommit(offset: Offset): F[Unit]
}
object PartitionContext {

  def apply[F[_]](implicit F: PartitionContext[F]): PartitionContext[F] = F

  def empty[F[_]: Applicative]: PartitionContext[F] = new PartitionContext[F] {
    def scheduleCommit(offset: Offset) = ().pure[F]
  }

  def of[F[_]: Ref.Make: Monad: Log](removeFromCache: F[Unit]): F[KeyContext[F]] =
    Ref.of[F, Option[Offset]](None) map { storage =>
      KeyContext(storage.stateInstance, removeFromCache)
    }

  def resource[F[_]: Sync](
    removeFromCache: F[Unit],
    log: Log[F]
  ): Resource[F, KeyContext[F]] = {
    implicit val _log = log
    Resource.eval(of(removeFromCache))
  }

}
