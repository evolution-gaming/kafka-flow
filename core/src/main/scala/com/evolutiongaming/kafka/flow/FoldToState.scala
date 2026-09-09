package com.evolutiongaming.kafka.flow

import cats.data.NonEmptyList
import cats.effect.{Ref, Sync}
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Monad, MonadThrow}
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.kafka.GenerationFencedError
import com.evolutiongaming.kafka.flow.persistence.Persistence

/** Applies records to a state stored inside and informs the listeners about the changes */
trait FoldToState[F[_], E] {

  def apply(records: NonEmptyList[E]): F[Unit]

}

object FoldToState {

  def of[F[_]: Sync: KeyContext, S, E](
    initialState: Option[S],
    fold: FoldOption[F, S, E],
    persistence: Persistence[F, S, E]
  ): F[FoldToState[F, E]] = Ref.of[F, Option[S]](initialState) map { storage =>
    FoldToState(storage.stateInstance, fold, persistence)
  }

  def apply[F[_]: Monad: KeyContext, S, E](
    storage: Stateful[F, Option[S]],
    fold: FoldOption[F, S, E],
    persistence: Persistence[F, S, E]
  ): FoldToState[F, E] = apply(storage, EnhancedFold.fromFold(fold), persistence, AdditionalStatePersist.empty[F, S, E])

  /** Uses `fold` to apply the records to a state stored inside of `storage`.
    *
    * Performs the necessary actions upon the state being changes, i.e. sends it to persistence, or removes the key if
    * the flow processing is finished.
    *
    * Every delete failure fails the flow here, a stale-generation fence included. Use the overload taking `remove` to
    * tolerate that one.
    */
  def apply[F[_]: Monad: KeyContext, S, E](
    storage: Stateful[F, Option[S]],
    fold: EnhancedFold[F, S, E],
    persistence: Persistence[F, S, E],
    additionalPersist: AdditionalStatePersist[F, S, E]
  ): FoldToState[F, E] =
    instance(storage, fold, persistence, additionalPersist, persistence.delete *> KeyContext[F].remove)

  /** As above, with `remove` as the effect that takes the key out of the partition once its state is deleted
    * ([[KeyFlow]] passes one that also stops the key's timers), and tolerating a delete the broker fenced.
    */
  def apply[F[_]: MonadThrow: KeyContext, S, E](
    storage: Stateful[F, Option[S]],
    fold: EnhancedFold[F, S, E],
    persistence: Persistence[F, S, E],
    additionalPersist: AdditionalStatePersist[F, S, E],
    remove: F[Unit],
  ): FoldToState[F, E] = instance(
    storage,
    fold,
    persistence,
    additionalPersist,
    // the fence is the rejection of the transaction that carried the tombstone: nothing landed, so the key is kept
    // and deleted again on the next tick. see the overload in `TickToState`, which retries it
    persistence.delete.attempt.flatMap {
      case Right(()) => remove
      case Left(e: GenerationFencedError) =>
        KeyContext[F].log.warn(s"delete fenced by a stale consumer generation, retrying on the next tick: $e")
      case Left(e) => e.raiseError[F, Unit]
    }
  )

  private def instance[F[_]: Monad, S, E](
    storage: Stateful[F, Option[S]],
    fold: EnhancedFold[F, S, E],
    persistence: Persistence[F, S, E],
    additionalPersist: AdditionalStatePersist[F, S, E],
    onStateEmptied: F[Unit],
  ): FoldToState[F, E] = new FoldToState[F, E] {
    private val keyFlowExtras = KeyFlowExtras.of(additionalPersist.request)

    def apply(records: NonEmptyList[E]): F[Unit] = {
      for {
        state <- storage.get
        state <- records.foldLeftM(state) { (state, record) =>
          fold(keyFlowExtras, state, record) flatTap { state =>
            for {
              _ <- persistence.appendEvent(record)
              _ <- state.traverse_ { state =>
                persistence.replaceState(state) >> additionalPersist.persistIfNeeded(record, state)
              }
            } yield ()
          }
        }
        _ <- storage set state

        // The reason why we do deletes at the end of the processing batch
        // is that it is possible that `fold(state, record)` returns `None`
        // in the middle of the batch.
        //
        // The typical situation when it happens is when ConsumerRecord without
        // actual events inside of it comes, so the state is changed from `None`
        // to `None`.
        //
        // This will cause the key to be deleted in `PartitionFlow`, but some
        // events applied after that causing the actual state to change from
        // `None` to `Some(_)` and this state will be lost, because the key
        // is already deleted from the cache.
        //
        // TODO: It is too unsafe to leave it like that. This will be partially
        // resolved by TECH-191, but, probably, we should do one of these things:
        // 1. Perform deletion only if `state` was `Some(_)` before that.
        // 2. Delay `KeyContext[F].remove` execution until the end of the batch
        //    inside of `PartitionFlow`.
        //
        // It makes me think that the initial implementation of returning `Done`
        // was not as bad as I thought.
        _ <- if (state.isEmpty) onStateEmptied else ().pure[F]
      } yield ()
    }
  }

}
