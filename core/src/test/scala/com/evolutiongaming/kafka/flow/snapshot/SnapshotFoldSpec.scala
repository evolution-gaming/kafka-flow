package com.evolutiongaming.kafka.flow.snapshot

import cats.Id
import com.evolutiongaming.kafka.flow.FoldOption
import com.evolutiongaming.kafka.journal.ConsRecord
import com.evolutiongaming.skafka.Offset
import com.evolutiongaming.skafka.TopicPartition
import munit.FunSuite

import SnapshotFoldSpec._

class SnapshotFoldSpec extends FunSuite {

  test("SnapshotFold updates KafkaSnapshot when there is no state") {
    val f = new ConstFixture
    val state = f.fold(
      s = None,
      a = ConsRecord(TopicPartition.empty, Offset.unsafe(1), None)
    )
    assert(state == Some(KafkaSnapshot(offset = Offset.unsafe(1), value = 100)))
  }

  test("SnapshotFold updates KafkaSnapshot when there is an existing state") {
    val f = new ConstFixture
    val state = f.fold(
      s = Some(KafkaSnapshot(offset = Offset.unsafe(1), value = 100)),
      a = ConsRecord(TopicPartition.empty, Offset.unsafe(2), None)
    )
    assert(state == Some(KafkaSnapshot(offset = Offset.unsafe(2), value = 200)))
  }

  test("SnapshotFold ignores duplicate update") {
    val f = new ConstFixture
    val state1 = f.fold(
      s = None,
      a = ConsRecord(TopicPartition.empty, Offset.unsafe(1), None)
    )
    val state2 = f.fold(
      s = state1,
      a = ConsRecord(TopicPartition.empty, Offset.unsafe(1), None)
    )
    assert(state1 == Some(KafkaSnapshot(offset = Offset.unsafe(1), value = 100)))
    assert(state2 == state1)
  }

}
object SnapshotFoldSpec {

  class ConstFixture {
    val fold = SnapshotFold[Id, Int](
      fold = FoldOption.modifyFold { state => state map (_ + 100) orElse Some(100) }
    )
  }

}