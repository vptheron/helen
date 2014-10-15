package io.helen.cql.frames.codecs

import java.util.UUID

import org.scalacheck.Arbitrary
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import scodec.bits.ByteVector

class BodySpec extends PropSpec with Matchers with PropertyChecks {

  import io.helen.cql.frames.codecs.Body._

  implicit val uuidArb = Arbitrary(org.scalacheck.Gen.uuid)

  property("Ints can be serialized and deserialized") {
    forAll { i: Int => readInt(writeInt(i)) shouldBe(i, ByteVector.empty)}
  }

  property("Longs can be serialized and deserialized") {
    forAll { l: Long => readLong(writeLong(l)) shouldBe(l, ByteVector.empty)}
  }

  property("Shorts can be serialized and deserialized") {
    forAll { s: Short => readShort(writeShort(s)) shouldBe(s, ByteVector.empty)}
  }

  property("Strings can be serialized and deserialized") {
    forAll { s: String => readString(writeString(s)) shouldBe(s, ByteVector.empty)}
  }

  property("Long strings can be serialized and deserialized") {
    forAll { s: String => readLongString(writeLongString(s)) shouldBe(s, ByteVector.empty)}
  }

  property("UUIDss can be serialized and deserialized") {
    forAll { u: UUID => readUuid(writeUuid(u)) shouldBe(u, ByteVector.empty)}
  }

  property("String Lists can be serialized and deserialized") {
    forAll { ss: List[String] => readStringList(writeStringList(ss)) shouldBe(ss, ByteVector.empty)}
  }

  property("Byte arrays can be serialized and deserialized") {
    forAll { data: Seq[Byte] =>
      val b = ByteVector(data)
      readBytes(writeBytes(b)) shouldBe(b, ByteVector.empty)
    }
  }

  property("Short byte arrays can be serialized and deserialized") {
    forAll { data: Seq[Byte] =>
      val b = ByteVector(data)
      readShortBytes(writeShortBytes(b)) shouldBe(b, ByteVector.empty)
    }
  }

//  property("InetSocketAddress can be serialized and deserialized") {
//    forAll { (host: String, port: Int) =>
//      val inet = new InetSocketAddress(host, Math.abs(port))
//      readInet(writeInet(inet)) shouldBe(inet, ByteVector.empty)
//    }
//  }

  property("String maps can be serialized and deserialized") {
    forAll { m: Map[String, String] => readStringMap(writeStringMap(m)) shouldBe(m, ByteVector.empty)}
  }

//  property("String multi-maps can be serialized and deserialized") {
//    forAll { m: Map[String, Seq[String]] => readStringMultiMap(writeStringMultiMap(m)) shouldBe(m, ByteVector.empty)}
//  }

}
