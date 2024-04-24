package com.evolutiongaming.kafka.flow.cassandra

import com.datastax.driver.core.SettableData
import com.evolutiongaming.scassandra.DecodeByName
import com.evolutiongaming.scassandra.EncodeByName
import com.evolutiongaming.skafka.TimestampType
import scala.jdk.CollectionConverters._
import scodec.bits.ByteVector
import com.evolutiongaming.skafka.Partition
import scala.util.Try
import com.evolutiongaming.skafka.Offset

private[flow] object CassandraCodecs {

  implicit val byteVectorEncodeByName: EncodeByName[ByteVector] = new EncodeByName[ByteVector] {
    def apply[B <: SettableData[B]](data: B, name: String, value: ByteVector) =
      data.setBytes(name, value.toByteBuffer)
  }
  implicit val byteVectorDecodeByName: DecodeByName[ByteVector] = DecodeByName[ByteVector] { (data, name) =>
    ByteVector(data.getBytes(name))
  }

  implicit val timestampTypeEncodeByName: EncodeByName[TimestampType] = new EncodeByName[TimestampType] {
    def apply[B <: SettableData[B]](data: B, name: String, value: TimestampType) = {
      val text = value match {
        case TimestampType.Create => "C"
        case TimestampType.Append => "A"
      }
      data.setString(name, text)
    }
  }
  implicit val timestampTypeDecodeByName: DecodeByName[TimestampType] = DecodeByName[TimestampType] { (data, name) =>
    data.getString(name) match {
      case "C" => TimestampType.Create
      case "A" => TimestampType.Append
    }
  }

  implicit val listStringEncodeByName: EncodeByName[List[String]] = {
    new EncodeByName[List[String]] {
      def apply[B <: SettableData[B]](data: B, name: String, values: List[String]) = {
        data.setList(name, values.asJava, classOf[String])
      }
    }
  }

  implicit val encodeByNamePartition: EncodeByName[Partition] = EncodeByName[Int].contramap { a: Partition => a.value }

  implicit val decodeByNamePartition: DecodeByName[Partition] = DecodeByName[Int].map { a => Partition.of[Try](a).get }

  implicit val encodeByNameOffset: EncodeByName[Offset] = EncodeByName[Long].contramap { a: Offset => a.value }

  implicit val decodeByNameOffset: DecodeByName[Offset] = DecodeByName[Long].map { a => Offset.of[Try](a).get }

}
