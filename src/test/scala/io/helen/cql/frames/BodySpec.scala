package io.helen.cql.frames.codecs

import java.util.UUID

import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}
import scodec.bits.ByteVector

class BodySpec extends PropSpec with Matchers with PropertyChecks {

  import Body._

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

//  property("UUIDss can be serialized and deserialized") {
//    forAll { u: UUID => readUuid(writeUuid(u)) shouldBe(u, ByteVector.empty)}
//  }

  property("String Lists can be serialized and deserialized") {
    forAll { ss: List[String] => readStringList(writeStringList(ss)) shouldBe(ss, ByteVector.empty)}
  }

//  property("Byte arrays can be serialized and deserialized") {
//    forAll { b: ByteVector => readBytes(writeBytes(b)) shouldBe(b, ByteVector.empty)}
//  }
//
//  property("Short byte arrays can be serialized and deserialized") {
//    forAll { b: ByteVector => readShortBytes(writeShortBytes(b)) shouldBe(b, ByteVector.empty)}
//  }

}
