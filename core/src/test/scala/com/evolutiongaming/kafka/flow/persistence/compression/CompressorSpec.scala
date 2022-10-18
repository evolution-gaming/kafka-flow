package com.evolutiongaming.kafka.flow.persistence.compression

import com.evolutiongaming.skafka.{FromBytes, ToBytes}
import munit.FunSuite
import scodec.bits.{BitVector, ByteVector}

import java.nio.charset.Charset
import scala.util.Try

class CompressorSpec extends FunSuite {
  import scodec.codecs._

  implicit val headerToBytes: ToBytes[Try, Header] =
    (h, _) => bool.encode(h.compressed).toTry.map(_.toByteArray)
  implicit val headerFromBytes: FromBytes[Try, Header] =
    (bytes, _) => bool.decode(BitVector(bytes)).map(dr => Header(dr.value)).toTry

  val compressor = Compressor.of[Try](compressionThreshold = 1).get

  test("compression below threshold") {
    val t = for {
      compressor <- Compressor.of[Try](compressionThreshold = 10000)
      bytes <- ByteVector.encodeString("test")(Charset.defaultCharset()).toTry
      compressed <- compressor.to(bytes)
      uncompressed <- compressor.from(compressed)
      uncompressedString <- uncompressed.decodeString(Charset.defaultCharset()).toTry
    } yield {
      assertEquals(compressed.length, 9L) // 4-byte string + metainformation
      assertEquals(uncompressed, bytes)
      assertEquals(uncompressedString, "test")
    }

    t.get
  }

  test("compression above threshold") {
    val t = for {
      compressor <- Compressor.of[Try](compressionThreshold = 1)
      bytes <- ByteVector.encodeString("test")(Charset.defaultCharset()).toTry
      compressed <- compressor.to(bytes)
      uncompressed <- compressor.from(compressed)
      uncompressedString <- uncompressed.decodeString(Charset.defaultCharset()).toTry
    } yield {
      assertEquals(uncompressed.length, bytes.length)
      assertEquals(uncompressedString, "test")

      assertEquals(compressed.length, 14L)
      assertNotEquals(compressed.length, bytes.length)
      assertNotEquals(compressed, bytes)
    }

    t.get
  }

  test("backward-compatibility support") {
    val t = for {
      compressor <- Compressor.of[Try](compressionThreshold = 1)
      bytes <- ByteVector.encodeString("""{"key":"value"}""")(Charset.defaultCharset()).toTry
      uncompressed <- compressor.from(bytes)
    } yield {
      assertEquals(uncompressed, bytes)
    }

    t.get
  }

}
