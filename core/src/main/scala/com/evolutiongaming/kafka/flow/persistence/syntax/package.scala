package com.evolutiongaming.kafka.flow.persistence

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.FoldOption
import cats.Foldable

package object syntax {

  implicit class FoldOptionPersistenceOps[F[_], S, A](val self: FoldOption[F, S, A]) extends AnyVal {

    /** Makes the fold persistent using provided implementation.
      *
      * I.e appends events and states to persistence and deletes the key
      * when state becomes `None`.
      */
    def persist(persistence: Persistence[F, S, A])(implicit F: Monad[F]): FoldOption[F, S, A] =
      FoldOption {
        self.value tap { (s, a) =>
          s map { s =>
            persistence.appendEvent(a) *> persistence.replaceState(s)
          } getOrElse {
            persistence.delete
          }
        }
      }

    /** Makes the fold persistent using provided implementation.
      *
      * The difference between this method and `persist` that it only
      * deletes the events / state after the batch of events processed.
      */
    def persistBatch[G[_]](
      persistence: Persistence[F, S, A]
    )(implicit F: Monad[F], G: Foldable[G]): FoldOption[F, S, G[A]] =
      FoldOption {
        val persistEvents = self.value.tap { (s, a) =>
          persistence.appendEvent(a) *> s.traverse_(persistence.replaceState)
        }
        persistEvents.batch[G].tap { (s, _) =>
          if (s.isEmpty) persistence.delete else ().pure[F]
        }
      }

  }

}
