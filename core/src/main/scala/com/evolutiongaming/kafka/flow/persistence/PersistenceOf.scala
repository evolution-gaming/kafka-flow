package com.evolutiongaming.kafka.flow.persistence

import cats.Applicative
import cats.Monad
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.flow.journal.Journals
import com.evolutiongaming.kafka.flow.journal.JournalsOf
import com.evolutiongaming.kafka.flow.key.KeysOf
import com.evolutiongaming.kafka.flow.snapshot.SnapshotsOf
import com.evolutiongaming.kafka.flow.timer.Timestamps

/** Creates generic persistence for a key */
trait PersistenceOf[F[_], K, S, A] {

  /** Creates generic persistence for a key.
    *
    * @param key key for which the values are stored into database,
    * @param fold recovery function to use if the state is restored from journal,
    * @param timerstamp service to register persistence events to.
    */
  def apply(
    key: K,
    fold: FoldOption[F, S, A],
    timestamps: Timestamps[F],
  ): F[Persistence[F, S, A]]

}
/** Creates snapshot persistence for a key.
  *
  * Ignores recovery function because the state is to be restored from snapshot.
  */
trait SnapshotPersistenceOf[F[_], K, S, A] extends PersistenceOf[F, K, S, A] {self =>

  def apply(
    key: K,
    timestamps: Timestamps[F],
  ): F[Persistence[F, S, A]]

  def apply(
    key: K,
    fold: FoldOption[F, S, A],
    timestamps: Timestamps[F],
  ): F[Persistence[F, S, A]] = apply(key, timestamps)

  final def contramap[B](f: B => K ): SnapshotPersistenceOf[F, B,S,A] = new SnapshotPersistenceOf[F, B,S,A] {
    override def apply(key: B, timestamps: Timestamps[F]): F[Persistence[F, S, A]] = self(f(key), timestamps)
  }
}
object PersistenceOf {

  /** Saves both events and snapshots, restores state from events */
  def restoreEvents[F[_]: Monad: LogOf, K, S, A](
    keysOf: KeysOf[F, K],
    journalsOf: JournalsOf[F, K, A],
    snapshotsOf: SnapshotsOf[F, K, S]
  ): Resource[F, PersistenceOf[F, K, S, A]] = {
    val log = LogOf[F].apply(PersistenceOf.getClass)
    Resource.liftF(log) map { implicit log => (key, fold, timestamps) =>
      implicit val _timestamps = timestamps
      for {
        journals <- journalsOf(key)
        snapshots <- snapshotsOf(key)
        keys = keysOf(key)
      } yield Persistence(
        readState = ReadState(journals, fold),
        buffers = Buffers(keys, journals, snapshots)
      )
    }
  }

  /** Saves both events and snapshots, restores state from snapshots */
  def restoreSnapshots[F[_]: Monad, K, S, A](
    keysOf: KeysOf[F, K],
    journalsOf: JournalsOf[F, K, A],
    snapshotsOf: SnapshotsOf[F, K, S],
  ): SnapshotPersistenceOf[F, K, S, A] = { (key, timestamps) =>
    implicit val _timestamps = timestamps
    for {
      journals <- journalsOf(key)
      snapshots <- snapshotsOf(key)
      keys = keysOf(key)
    } yield Persistence(
      readState = ReadState(snapshots),
      buffers = Buffers(keys, journals, snapshots),
    )
  }

  /** Saves snapshots only, restores state from snapshots */
  def snapshotsOnly[F[_]: Monad, K, S, A](
    keysOf: KeysOf[F, K],
    snapshotsOf: SnapshotsOf[F, K, S],
  ): SnapshotPersistenceOf[F, K, S, A] = { (key, timestamps) =>
    implicit val _timestamps = timestamps
    for {
      snapshots <- snapshotsOf(key)
      keys = keysOf(key)
    } yield Persistence(
      readState = ReadState(snapshots),
      buffers = Buffers(keys, Journals.empty[F, A], snapshots),
    )
  }

  /** No actual persistence is performed */
  def empty[F[_]: Applicative, K, S, A]: PersistenceOf[F, K, S, A] = { (_, _, _) =>
    Persistence.empty[F, S, A].pure[F]
  }

}
