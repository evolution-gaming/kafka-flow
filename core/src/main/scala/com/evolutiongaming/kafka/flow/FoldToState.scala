package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.syntax.all._
import cats.mtl.MonadState
import com.olegpy.meow.effects._
import persistence.Persistence
import cats.effect.Ref

/** Applies records to a state stored inside and informs the listeners about the changes */
trait FoldToState[F[_], E] {

  def apply(records: NonEmptyList[E]): F[Unit]

}

object FoldToState {

  def of[F[_]: Sync: KeyContext, S, E](
    initialState: Option[S],
    fold: FoldOption[F, S, E],
    persistence: Persistence[F, S, E]
  ): F[FoldToState[F, E]] = Ref.of(initialState) map { storage =>
    FoldToState(storage.stateInstance, fold, persistence)
  }

  def apply[F[_]: Monad: KeyContext, S, E](
    storage: MonadState[F, Option[S]],
    fold: FoldOption[F, S, E],
    persistence: Persistence[F, S, E]
  ): FoldToState[F, E] = apply(storage, EnhancedFold.fromFold(fold), persistence, AdditionalStatePersist.empty[F, E])

  /** Uses `fold` to apply the records to a state stored inside of `storage`.
    *
    * Performs the necessary actions upon the state being changes, i.e.
    * sends it to persistence, or removes the key if the flow processing
    * is finished.
    */
  def apply[F[_]: Monad: KeyContext, S, E](
                                            storage: MonadState[F, Option[S]],
                                            fold: EnhancedFold[F, S, E],
                                            persistence: Persistence[F, S, E],
                                            additionalPersist: AdditionalStatePersist[F, E]
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
                persistence.replaceState(state) >> additionalPersist.persistIfNeeded(record)
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
        _ <-
          if (state.isEmpty) {
            persistence.delete *> KeyContext[F].remove
          } else {
            ().pure[F]
          }
      } yield ()
    }
  }

}
