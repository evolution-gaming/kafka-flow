package com.evolutiongaming.kafka.flow

import cats.Monad
import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances._
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
    */
  def apply[F[_]: Monad: KeyContext, S](
    storage: Stateful[F, Option[S]],
    tick: TickOption[F, S],
    persistence: Persistence[F, S, _]
  ): TickToState[F] = new TickToState[F] {
    def run = for {
      state <- storage.get
      state <- tick(state)
      _     <- state traverse_ persistence.replaceState
      _     <- storage set state
      _ <-
        if (state.isEmpty) {
          persistence.delete *> KeyContext[F].remove
        } else {
          ().pure[F]
        }
    } yield ()
  }

}
