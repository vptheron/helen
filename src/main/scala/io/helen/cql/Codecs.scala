/*
 *      Copyright (C) 2014 Vincent Theron
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package io.helen.cql

import java.net.InetAddress
import java.util.UUID

import akka.util.{ByteStringBuilder, ByteString}
import org.joda.time.DateTime

object Codecs {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def asAscii(s: String): ByteString = ByteString.fromString(s, "ASCII")

  def asText(t: String): ByteString = ByteString.fromString(t)

  def asVarchar(s: String): ByteString = asText(s)

  def asBigInt(l: Long): ByteString = new ByteStringBuilder().putLong(l).result()

  def asCounter(l: Long): ByteString = asBigInt(l)

  def asBoolean(b: Boolean): ByteString = ByteString(if (b) 1 else 0)

  def asDecimal(d: BigDecimal): ByteString = {
    val unscaledValue = d.bigDecimal.unscaledValue().toByteArray
    val scale = d.bigDecimal.scale()
    new ByteStringBuilder().putInt(scale).putBytes(unscaledValue).result()
  }

  def asDouble(d: Double): ByteString = new ByteStringBuilder().putDouble(d).result()

  def asFloat(f: Float): ByteString = new ByteStringBuilder().putFloat(f).result()

  def asInet(i: InetAddress): ByteString = ByteString(i.getAddress)

  def asInt(i: Int): ByteString = new ByteStringBuilder().putInt(i).result()

  def asTimestamp(d: DateTime): ByteString = asBigInt(d.getMillis)

  def asUUID(u: UUID): ByteString = {
    new ByteStringBuilder()
      .putLong(u.getMostSignificantBits)
      .putLong(u.getLeastSignificantBits)
      .result()
  }

  def asVarint(i: BigInt): ByteString = ByteString(i.toByteArray)

  def asTimeUUID(u: UUID): ByteString = asUUID(u)

  def asList[A](l: List[A], eltCodec: A => ByteString): ByteString = {
    val builder = new ByteStringBuilder()
      .putInt(l.size)

    l.foreach(eltCodec andThen Body.bytes andThen builder.append)
    builder.result()
  }

  def asSet[A](s: Set[A], eltCodec: A => ByteString): ByteString = {
    val builder = new ByteStringBuilder()
      .putInt(s.size)

    s.foreach(eltCodec andThen Body.bytes andThen builder.append)
    builder.result()
  }

  def asMap[K, V](m: Map[K, V], keyCodec: K => ByteString, valueCodec: V => ByteString): ByteString = {
    val builder = new ByteStringBuilder()
      .putInt(m.size)

    m foreach {
      case (key, value) => builder
        .append(Body.bytes(keyCodec(key)))
        .append(Body.bytes(valueCodec(value)))
    }
    builder.result()
  }


  def fromAscii(b: ByteString): String = b.decodeString("ASCII")

  def fromBigInt(b: ByteString): Long = b.iterator.getLong

  def fromBoolean(b: ByteString): Boolean = b(0) == 1

  def fromCounter(b: ByteString): Long = fromBigInt(b)

  def fromDecimal(b: ByteString): BigDecimal = {
    val it = b.iterator
    val scale = it.getInt
    val biBytes = it.toArray
    BigDecimal(BigInt(biBytes), scale)
  }

  def fromDouble(b: ByteString): Double = b.iterator.getDouble

  def fromFloat(b: ByteString): Float = b.iterator.getFloat

  def fromInet(b: ByteString): InetAddress = InetAddress.getByAddress(b.toArray)

  def fromInt(b: ByteString): Int = b.iterator.getInt

  def fromText(b: ByteString): String = b.utf8String

  def fromTimestamp(b: ByteString): DateTime = new DateTime(fromBigInt(b))

  def fromUUID(b: ByteString): UUID = {
    val it = b.iterator
    new UUID(it.getLong, it.getLong)
  }

  def fromVarchar(b: ByteString): String = fromText(b)

  def fromVarint(b: ByteString): BigInt = BigInt(b.toArray)

  def fromTimeUUID(b: ByteString): UUID = {
    val id = fromUUID(b)
    if (id.version != 1)
      throw new Exception("Deserialized value is not a valid TimeUUID.")
    id
  }

  def fromList[A](b: ByteString, eltCodec: ByteString => A): List[A] = {
    val it = b.iterator
    val size = it.getInt

    (0 until size).map(_ => eltCodec(Body.readBytes(it).get))
      .toList
  }

  def fromSet[A](b: ByteString, eltCodec: ByteString => A): Set[A] = {
    val it = b.iterator
    val size = it.getInt

    (0 until size).map(_ => eltCodec(Body.readBytes(it).get))
      .toSet
  }

  def fromMap[K, V](b: ByteString, keyCodec: ByteString => K, valueCode: ByteString => V): Map[K, V] = {
    val it = b.iterator
    val size = it.getInt

    (0 until size).map(_ => keyCodec(Body.readBytes(it).get) -> valueCode(Body.readBytes(it).get))
      .toMap
  }

  object Implicits {

    implicit class ByteStringCodec(val bs: ByteString) extends AnyVal {

      def fromAscii: String = Codecs.fromAscii(bs)

      def fromBigInt: Long = Codecs.fromBigInt(bs)

      def fromBoolean: Boolean = Codecs.fromBoolean(bs)

      def fromCounter: Long = Codecs.fromCounter(bs)

      def fromDecimal: BigDecimal = Codecs.fromDecimal(bs)

      def fromDouble: Double = Codecs.fromDouble(bs)

      def fromFloat: Float = Codecs.fromFloat(bs)

      def fromInet: InetAddress = Codecs.fromInet(bs)

      def fromInt: Int = Codecs.fromInt(bs)

      def fromText: String = Codecs.fromText(bs)

      def fromTimestamp: DateTime = Codecs.fromTimestamp(bs)

      def fromUUID: UUID = Codecs.fromUUID(bs)

      def fromVarchar: String = Codecs.fromVarchar(bs)

      def fromVarint: BigInt = Codecs.fromVarint(bs)

      def fromTimeUUID: UUID = Codecs.fromTimeUUID(bs)
    }

    implicit class StringCodec(val s: String) extends AnyVal {

      def asAscii: ByteString = Codecs.asAscii(s)

      def asText: ByteString = Codecs.asText(s)

      def asVarchar: ByteString = Codecs.asVarchar(s)

    }

    implicit class LongCodec(val l: Long) extends AnyVal {

      def asBigInt: ByteString = Codecs.asBigInt(l)

      def asCounter: ByteString = asBigInt

    }

    implicit class BooleanCodec(val b: Boolean) extends AnyVal {

      def asBoolean: ByteString = Codecs.asBoolean(b)

    }

    implicit class BigDecimalCodec(val bd: BigDecimal) extends AnyVal {

      def asDecimal: ByteString = Codecs.asDecimal(bd)

    }

    implicit class DoubleCodec(val d: Double) extends AnyVal {

      def asDouble: ByteString = Codecs.asDouble(d)

    }

    implicit class FloatCodec(val f: Float) extends AnyVal {

      def asFloat: ByteString = Codecs.asFloat(f)

    }

    implicit class InetCodec(val inet: InetAddress) extends AnyVal {

      def asInet: ByteString = Codecs.asInet(inet)

    }

    implicit class IntCodec(val i: Int) extends AnyVal {

      def asInt: ByteString = Codecs.asInt(i)

    }

    implicit class DateTimeCodec(val dt: DateTime) extends AnyVal {

      def asTimestamp: ByteString = Codecs.asTimestamp(dt)

    }

    implicit class UUIDCodec(val uuid: UUID) extends AnyVal {

      def asUUID: ByteString = Codecs.asUUID(uuid)

      def asTimeUUID: ByteString = Codecs.asTimeUUID(uuid)

    }

    implicit class BigIntCodec(val bi: BigInt) extends AnyVal {

      def asVarint: ByteString = Codecs.asVarint(bi)

    }

  }

}
