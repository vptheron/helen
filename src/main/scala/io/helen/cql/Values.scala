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

object Values {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def asciiToBytes(s: String): ByteString = ByteString.fromString(s, "ASCII")

  def bytesToAscii(b: ByteString): String = b.decodeString("ASCII")

  def bigIntToBytes(l: Long): ByteString = new ByteStringBuilder().putLong(l).result()

  def bytesToBigInt(b: ByteString): Long = b.iterator.getLong

  def booleanToBytes(b: Boolean): ByteString = ByteString(if (b) 1 else 0)

  def bytesToBoolean(b: ByteString): Boolean = b(0) == 1

  def counterToBytes(l: Long): ByteString = bigIntToBytes(l)

  def bytesToCounter(b: ByteString): Long = bytesToBigInt(b)

  def decimalToBytes(d: BigDecimal): ByteString = {
    val unscaledValue = d.bigDecimal.unscaledValue().toByteArray
    val scale = d.bigDecimal.scale()
    new ByteStringBuilder().putInt(scale).putBytes(unscaledValue).result()
  }

  def bytesToDecimal(b: ByteString): BigDecimal = {
    val it = b.iterator
    val scale = it.getInt
    val biBytes = it.toArray
    BigDecimal(BigInt(biBytes), scale)
  }

  def doubleToBytes(d: Double): ByteString = new ByteStringBuilder().putDouble(d).result()

  def bytesToDouble(b: ByteString): Double = b.iterator.getDouble

  def floatToBytes(f: Float): ByteString = new ByteStringBuilder().putFloat(f).result()

  def bytesToFloat(b: ByteString): Float = b.iterator.getFloat

  def inetToBytes(i: InetAddress): ByteString = ByteString(i.getAddress)

  def bytesToInet(b: ByteString): InetAddress = InetAddress.getByAddress(b.toArray)

  def intToBytes(i: Int): ByteString = new ByteStringBuilder().putInt(i).result()

  def bytesToInt(b: ByteString): Int = b.iterator.getInt

  def textToBytes(t: String): ByteString = ByteString.fromString(t)

  def bytesToText(b: ByteString): String = b.utf8String

  def dateTimeToBytes(d: DateTime): ByteString = bigIntToBytes(d.getMillis)

  def bytesToDateTime(b: ByteString): DateTime = new DateTime(bytesToBigInt(b))

  def uuidToBytes(u: UUID): ByteString = {
    new ByteStringBuilder()
      .putLong(u.getMostSignificantBits)
      .putLong(u.getLeastSignificantBits)
      .result()
  }

  def bytesToUUID(b: ByteString): UUID = {
    val it = b.iterator
    new UUID(it.getLong, it.getLong)
  }

  def varcharToBytes(s: String): ByteString = textToBytes(s)

  def bytesToVarchar(b: ByteString): String = bytesToText(b)

  def varIntToBytes(i: BigInt): ByteString = ByteString(i.toByteArray)

  def bytesToVarInt(b: ByteString): BigInt = BigInt(b.toArray)

  def timeUUIDToBytes(u: UUID): ByteString = uuidToBytes(u)

  def bytesToTimeUUID(b: ByteString): UUID = {
    val id = bytesToUUID(b)
    if (id.version != 1)
      throw new Exception("Deserialized value is not a valid TimeUUID.")
    id
  }

  def listToBytes[A](l: List[A], eltCodec: A => ByteString): ByteString = {
    val builder = new ByteStringBuilder()
      .putShort(l.size)

    l.foreach(eltCodec andThen Body.shortBytes andThen builder.append)
    builder.result()
  }

  def bytesToList[A](b: ByteString, eltCodec: ByteString => A): List[A] = {
    val it = b.iterator
    val size = it.getShort

    (0 until size).map(_ => eltCodec(Body.readShortBytes(it)))
      .toList
  }

  def setToBytes[A](s: Set[A], eltCodec: A => ByteString): ByteString = {
    val builder = new ByteStringBuilder()
      .putShort(s.size)

    s.foreach(eltCodec andThen Body.shortBytes andThen builder.append)
    builder.result()
  }

  def bytesToSet[A](b: ByteString, eltCodec: ByteString => A): Set[A] = {
    val it = b.iterator
    val size = it.getShort

    (0 until size).map(_ => eltCodec(Body.readShortBytes(it)))
      .toSet
  }

  def mapToBytes[K, V](m: Map[K, V], keyCodec: K => ByteString, valueCodec: V => ByteString): ByteString = {
    val builder = new ByteStringBuilder()
      .putShort(m.size)

    m foreach {
      case (key, value) => builder
        .append(Body.shortBytes(keyCodec(key)))
        .append(Body.shortBytes(valueCodec(value)))
    }
    builder.result()
  }

  def bytesToMap[K, V](b: ByteString, keyCodec: ByteString => K, valueCode: ByteString => V): Map[K, V] = {
    val it = b.iterator
    val size = it.getShort

    (0 until size).map(_ => keyCodec(Body.readShortBytes(it)) -> valueCode(Body.readShortBytes(it)))
      .toMap
  }

}
