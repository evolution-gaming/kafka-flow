package com.evolutiongaming.kafka.flow.kafka

import scala.util.control.NoStackTrace

/** A transactional offset commit the broker rejected for a stale consumer generation (KIP-447; kafka-clients raises it
  * as `CommitFailedException`), aborting the transaction that carried it and any snapshot write in it. Nothing landed,
  * no offset advanced.
  *
  * The rejection is itself the fence, so callers tolerate it instead of failing the flow: the key stays dirty or the
  * offset uncommitted, and the next tick retries under the generation the consumer refreshes once it completes the
  * rebalance. See `docs/kafka-single-writer-design.md`.
  */
final case class GenerationFencedError(cause: Throwable) extends RuntimeException(cause) with NoStackTrace
