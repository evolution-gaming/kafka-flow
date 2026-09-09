package com.evolutiongaming.kafka.flow

import cats.effect.{Ref, SyncIO}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.kafka.flow.kafka.GenerationFencedError
import com.evolutiongaming.kafka.flow.persistence.Persistence
import com.evolutiongaming.skafka.Offset
import munit.FunSuite

class TickToStateSpec extends FunSuite {

  test("a state the tick empties is deleted and the key removed") {
    val f = new ConstFixture(deleteFailures = Nil)
    f.tickToState.run.unsafeRunSync()
    assertEquals(f.deletes.get.unsafeRunSync(), 1)
    assertEquals(f.removes.get.unsafeRunSync(), 1)
    assertEquals(f.storage.get.unsafeRunSync(), None)
  }

  test("a fenced delete keeps the key, and the next tick deletes again") {
    val f = new ConstFixture(deleteFailures = List(GenerationFencedError(new Exception("stale generation"))))

    // the fence is tolerated: nothing was deleted and the key is not removed, so it keeps holding its offset and,
    // since KeyFlow stops a key's timers on removal rather than on an empty state, gets a next tick
    f.tickToState.run.unsafeRunSync()
    assertEquals(f.deletes.get.unsafeRunSync(), 1)
    assertEquals(f.removes.get.unsafeRunSync(), 0)
    assertEquals(f.storage.get.unsafeRunSync(), None)

    // the consumer has refreshed its generation by now: the delete lands and the key goes
    f.tickToState.run.unsafeRunSync()
    assertEquals(f.deletes.get.unsafeRunSync(), 2)
    assertEquals(f.removes.get.unsafeRunSync(), 1)
  }

  test("any other delete failure still fails the tick") {
    val f = new ConstFixture(deleteFailures = List(new RuntimeException("broker down")))
    intercept[RuntimeException](f.tickToState.run.unsafeRunSync())
    assertEquals(f.removes.get.unsafeRunSync(), 0)
  }

  class ConstFixture(deleteFailures: List[Throwable]) {
    val deletes: Ref[SyncIO, Int]              = Ref.unsafe[SyncIO, Int](0)
    val removes: Ref[SyncIO, Int]              = Ref.unsafe[SyncIO, Int](0)
    val failures: Ref[SyncIO, List[Throwable]] = Ref.unsafe[SyncIO, List[Throwable]](deleteFailures)
    val storage: Ref[SyncIO, Option[Int]]      = Ref.unsafe[SyncIO, Option[Int]](Some(1))

    implicit val keyContext: KeyContext[SyncIO] = new KeyContext[SyncIO] {
      def holding              = none[Offset].pure[SyncIO]
      def hold(offset: Offset) = SyncIO.unit
      def remove               = removes.update(_ + 1)
      def log                  = Log.empty
    }

    val persistence: Persistence[SyncIO, Int, String] = new Persistence[SyncIO, Int, String] {
      def read                       = none[Int].pure[SyncIO]
      def flush                      = SyncIO.unit
      def appendEvent(event: String) = SyncIO.unit
      def replaceState(state: Int)   = SyncIO.unit
      def delete = deletes.update(_ + 1) *> failures.modify {
        case e :: rest => (rest, e.raiseError[SyncIO, Unit])
        case Nil       => (Nil, SyncIO.unit)
      }.flatten
    }

    // an eviction tick: whatever the state, the key is to go
    val evict: TickOption[SyncIO, Int] = TickOption.of(_ => none[Int].pure[SyncIO])

    val tickToState: TickToState[SyncIO] =
      TickToState(storage.stateInstance, evict, persistence, KeyContext[SyncIO].remove)
  }

}
