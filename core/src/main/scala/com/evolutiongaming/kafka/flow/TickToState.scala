package com.evolutiongaming.kafka.flow

import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Monad, MonadThrow}
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.kafka.GenerationFencedError
import com.evolutiongaming.kafka.flow.persistence.Persistence

/** Calls the stateful routine stored inside */
trait TickToState[F[_]] {

  def run: F[Unit]

}

object TickToState {

  def of[F[_]: Monad: Ref.Make: KeyContext, S](
    initialState: Option[S],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, _]
  ): F[TickToState[F]] = Ref.of(initialState) map { storage =>
    TickToState(storage.stateInstance, tick, persistence)
  }

  /** Uses `tick` to call the effect on a state stored inside of `storage`.
    *
    * Performs the necessary actions upon the state being changes, i.e. sends it to persistence, or removes the key if
    * the flow processing is finished.
    *
    * Every delete failure fails the flow here, a stale-generation fence included. Use the overload taking `remove` to
    * tolerate that one.
    */
  def apply[F[_]: Monad: KeyContext, S](
    storage: Stateful[F, Option[S]],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, _]
  ): TickToState[F] = instance(storage, tick, persistence, persistence.delete *> KeyContext[F].remove)

  /** As above, with `remove` as the effect that takes the key out of the partition once its state is deleted
    * ([[KeyFlow]] passes one that also stops the key's timers), and tolerating a delete the broker fenced.
    */
  def apply[F[_]: MonadThrow: KeyContext, S](
    storage: Stateful[F, Option[S]],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, _],
    remove: F[Unit],
  ): TickToState[F] = instance(
    storage,
    tick,
    persistence,
    // a transactional delete is a Kafka transaction the broker rejects for a stale consumer generation. the rejection
    // is the fence: nothing landed and the consumer refreshes its generation once it completes the rebalance, so the
    // key is kept and the next tick deletes again. removing it now would drop its held offset and let the partition
    // commit past the undeleted snapshot. every other error fails the flow
    persistence.delete.attempt.flatMap {
      case Right(()) => remove
      case Left(e: GenerationFencedError) =>
        KeyContext[F].log.warn(s"delete fenced by a stale consumer generation, retrying on the next tick: $e")
      case Left(e) => e.raiseError[F, Unit]
    }
  )

  private def instance[F[_]: Monad, S](
    storage: Stateful[F, Option[S]],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, _],
    onStateEmptied: F[Unit],
  ): TickToState[F] = new TickToState[F] {
    def run = for {
      state <- storage.get
      state <- tick(state)
      _     <- state traverse_ persistence.replaceState
      _     <- storage set state
      _     <- if (state.isEmpty) onStateEmptied else ().pure[F]
    } yield ()
  }

}
