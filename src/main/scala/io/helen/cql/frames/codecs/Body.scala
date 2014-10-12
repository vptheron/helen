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
package io.helen.cql.frames.codecs

import java.net.{InetSocketAddress, InetAddress}
import java.util.UUID

import scodec.bits.ByteVector

import scala.util.Try

object Body {

  def fromInt(i: Int): ByteVector = ByteVector.fromInt(i)

  def toInt(data: ByteVector): Try[(Int, ByteVector)] = Try {
    data.
    val (iAsBytes, rem) = data.splitAt(4)
    ByteVector(iAsBytes).toInt() -> rem
  }

  def fromLong(l: Long): ByteSeq = ByteVector.fromLong(l).toSeq

  def toLong(data: ByteSeq): Try[(Long, ByteSeq)] = Try {
    val (lAsBytes, rem) = data.splitAt(8)
    ByteVector(lAsBytes).toLong() -> rem
  }

  def fromShort(s: Short): ByteSeq = ByteVector.fromShort(s).toSeq

  def toShort(data: ByteSeq): Try[(Short, ByteSeq)] = Try {
    val (sAsBytes, rem) = data.splitAt(2)
    ByteVector(sAsBytes).toShort() -> rem
  }

  def fromString(s: String): ByteSeq = {
    val sAsBytes = ByteVector(s.getBytes("UTF-8")).toSeq
    fromShort(sAsBytes.length.toShort) ++ sAsBytes
  }

  def toString(data: ByteSeq): Try[(String, ByteSeq)] = for {
    (stringSize, rem) <- toShort(data)
    (stringAsBytes, left) <- rem.splitAt(stringSize)
  } yield new String(stringAsBytes, "UTF-8") -> left

  def fromLongString(s: String): ByteSeq = {
    val sAsBytes = ByteVector(s.getBytes("UTF-8")).toSeq
    fromInt(sAsBytes.length) ++ sAsBytes
  }

  def toLongString(data: ByteSeq): Try[(String, ByteSeq)] = for {
    (stringSize, rem) <- toInt(data)
    (stringAsBytes, left) <- rem.splitAt(stringSize)
  } yield new String(stringAsBytes, "UTF-8") -> left

  def fromUuid(id: UUID): ByteSeq =
    (ByteVector.fromLong(id.getMostSignificantBits) ++ ByteVector.fromLong(id.getLeastSignificantBits)).toSeq

  def toUuid(data: ByteSeq): Try[(UUID, ByteSeq)] = for {
    (mostSignBits, rest) <- toLong(data)
    (leastSignBits, rest2) <- toLong(rest)
  } yield new UUID(mostSignBits, leastSignBits) -> rest2.toSeq

  def fromStringList(ss: Seq[String]): ByteSeq = {
    (ByteVector.fromShort(ss.length.toShort) ++
      ss.foldLeft(ByteVector.empty)((acc, s) => acc ++ ByteVector(fromString(s)))
      ).toSeq
  }

  def toStringList(data: ByteSeq): Try[(Seq[String], ByteSeq)] = for {
    (listSize, rest) <- toShort(data)

  }

//    Try {
//    val it = ByteString(data).iterator
//    val listLength = it.getShort
//    (0 until listLength).map(_ => toString(dataIterator))
//  }

  def bytes(b: ByteString): ByteString = {
    new ByteStringBuilder()
      .putInt(b.length)
      .append(b)
      .result()
  }

  def readBytes(dataIterator: ByteIterator): Option[ByteString] = {
    val size = dataIterator.getInt
    if (size < 0)
      None
    else {
      val buffer = new Array[Byte](size)
      dataIterator.getBytes(buffer)
      Some(ByteString(buffer))
    }
  }

  def shortBytes(b: ByteString): ByteString = {
    new ByteStringBuilder()
      .putShort(b.length)
      .append(b)
      .result()
  }

  def readShortBytes(dataIterator: ByteIterator): ByteString = {
    val buffer = new Array[Byte](dataIterator.getShort)
    dataIterator.getBytes(buffer)
    ByteString(buffer)
  }

  //TODO option

  //TODO option list

  def address(a: InetSocketAddress): ByteString = {
    val ip = a.getAddress.getAddress
    new ByteStringBuilder()
      .putByte(ip.length.toByte)
      .putBytes(ip)
      .putInt(a.getPort)
      .result()
  }

  def readAddress(dataIterator: ByteIterator): InetSocketAddress = {
    val buffer = new Array[Byte](dataIterator.getByte)
    dataIterator.getBytes(buffer)
    val ip = InetAddress.getByAddress(buffer)
    new InetSocketAddress(ip, dataIterator.getInt)
  }

  def stringMap(m: Map[String, String]): ByteString = {
    val builder = new ByteStringBuilder()
    builder.putShort(m.size)
    m.foreach(kv => builder.append(string(kv._1)).append(string(kv._2)))
    builder.result()
  }

  def readStringMap(dataIterator: ByteIterator): Map[String, String] = {
    val mapSize = dataIterator.getShort
    (0 until mapSize)
      .map(_ => readString(dataIterator) -> readString(dataIterator))
      .toMap
  }

  def stringMultiMap(m: Map[String, Seq[String]]): ByteString = {
    val builder = new ByteStringBuilder()
    builder.putShort(m.size)
    m.foreach(kv => builder.append(string(kv._1)).append(stringList(kv._2)))
    builder.result()
  }

  def readStringMultiMap(dataIterator: ByteIterator): Map[String, Seq[String]] = {
    val mapSize = dataIterator.getShort
    (0 until mapSize)
      .map(_ => readString(dataIterator) -> readStringList(dataIterator))
      .toMap
  }

}
