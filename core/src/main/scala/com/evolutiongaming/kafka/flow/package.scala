package com.evolutiongaming.kafka

import com.evolutiongaming.kafka.journal.ConsRecord

package object flow {
  type FoldCons[F[_], S]       = Fold[F, S, ConsRecord]
  type FoldOptionCons[F[_], S] = FoldOption[F, S, ConsRecord]
}
