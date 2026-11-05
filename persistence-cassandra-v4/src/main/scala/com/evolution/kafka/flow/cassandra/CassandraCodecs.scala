package com.evolution.kafka.flow.cassandra

import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}
import com.evolution.scassandra.{DecodeByName, EncodeByName}
import com.evolutiongaming.skafka.{Offset, Partition, TimestampType}
import scodec.bits.ByteVector

import scala.jdk.CollectionConverters.*
import scala.util.Try

private[flow] object CassandraCodecs {

  implicit val byteVectorEncodeByName: EncodeByName[ByteVector] = new EncodeByName[ByteVector] {
    def apply[B <: SettableByName[B]](data: B, name: String, value: ByteVector): B =
      data.setByteBuffer(name, value.toByteBuffer)
  }
  implicit val byteVectorDecodeByName: DecodeByName[ByteVector] = (data: GettableByName, name: String) =>
    ByteVector(data.getByteBuffer(name))

  implicit val timestampTypeEncodeByName: EncodeByName[TimestampType] = new EncodeByName[TimestampType] {
    def apply[B <: SettableByName[B]](data: B, name: String, value: TimestampType): B = {
      val text = value match {
        case TimestampType.Create => "C"
        case TimestampType.Append => "A"
      }
      data.setString(name, text)
    }
  }
  implicit val timestampTypeDecodeByName: DecodeByName[TimestampType] = (data: GettableByName, name: String) =>
    data.getString(name) match {
      case "C" => TimestampType.Create
      case "A" => TimestampType.Append
    }

  implicit val listStringEncodeByName: EncodeByName[List[String]] = new EncodeByName[List[String]] {
    def apply[B <: SettableByName[B]](data: B, name: String, values: List[String]): B =
      data.setList(name, values.asJava, classOf[String])
  }

  implicit val mapTextEncodeByName: EncodeByName[Map[String, String]] = {
    val text = classOf[String]
    new EncodeByName[Map[String, String]] {
      def apply[B <: SettableByName[B]](data: B, name: String, value: Map[String, String]): B =
        data.setMap(name, value.asJava, text, text)
    }
  }

  implicit val mapTextDecodeByName: DecodeByName[Map[String, String]] = {
    val text = classOf[String]
    (data: GettableByName, name: String) => data.getMap(name, text, text).asScala.toMap
  }

  implicit val encodeByNamePartition: EncodeByName[Partition] = EncodeByName[Int].contramap { (a: Partition) =>
    a.value
  }

  implicit val decodeByNamePartition: DecodeByName[Partition] = DecodeByName[Int].map { a => Partition.of[Try](a).get }

  implicit val encodeByNameOffset: EncodeByName[Offset] = EncodeByName[Long].contramap { (a: Offset) => a.value }

  implicit val decodeByNameOffset: DecodeByName[Offset] = DecodeByName[Long].map { a => Offset.of[Try](a).get }

}
