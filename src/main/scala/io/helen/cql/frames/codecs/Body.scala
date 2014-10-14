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

object Body {

  def writeInt(i: Int): ByteVector = ByteVector.fromInt(i)

  def readInt(data: ByteVector): (Int, ByteVector) = {
    val (iAsBytes, rem) = data.splitAt(4)
    iAsBytes.toInt() -> rem
  }

  def writeLong(l: Long): ByteVector = ByteVector.fromLong(l)

  def readLong(data: ByteVector): (Long, ByteVector) = {
    val (lAsBytes, rem) = data.splitAt(8)
    lAsBytes.toLong() -> rem
  }

  def writeShort(s: Short): ByteVector = ByteVector.fromShort(s)

  def readShort(data: ByteVector): (Short, ByteVector) = {
    val (sAsBytes, rem) = data.splitAt(2)
    sAsBytes.toShort() -> rem
  }

  def writeString(s: String): ByteVector = {
    val sAsBytes = ByteVector(s.getBytes("UTF-8"))
    writeShort(sAsBytes.length.toShort) ++ sAsBytes
  }

  def readString(data: ByteVector): (String, ByteVector) = {
    val (stringSize, rem1) = readShort(data)
    val (stringAsBytes, rem2) = rem1.splitAt(stringSize)
    new String(stringAsBytes.toArray, "UTF-8") -> rem2
  }

  def writeLongString(s: String): ByteVector = {
    val sAsBytes = ByteVector(s.getBytes("UTF-8"))
    writeInt(sAsBytes.length) ++ sAsBytes
  }

  def readLongString(data: ByteVector): (String, ByteVector) = {
    val (stringSize, rem1) = readInt(data)
    val (stringAsBytes, rem2) = rem1.splitAt(stringSize)
    new String(stringAsBytes.toArray, "UTF-8") -> rem2
  }

  def writeUuid(id: UUID): ByteVector =
    ByteVector.fromLong(id.getMostSignificantBits) ++ ByteVector.fromLong(id.getLeastSignificantBits)

  def readUuid(data: ByteVector): (UUID, ByteVector) = {
    val (mostSignBits, rem1) = readLong(data)
    val (leastSignBits, rem2) = readLong(rem1)
    new UUID(mostSignBits, leastSignBits) -> rem2
  }

  def writeStringList(ss: Seq[String]): ByteVector =
    ByteVector.fromShort(ss.length.toShort) ++
      ss.foldLeft(ByteVector.empty)((acc, s) => acc ++ writeString(s))

  def readStringList(data: ByteVector): (Seq[String], ByteVector) = {
    val (listSize, rest) = readShort(data)

    def loop(itemsToRead: Int, acc: Seq[String], rem: ByteVector): (Seq[String], ByteVector) = itemsToRead match {
      case 0 => (acc.reverse, rem)
      case _ =>
        val (newString, newRem) = readString(rem)
        loop(itemsToRead - 1, newString +: acc, newRem)
    }

    loop(listSize, Nil, rest)
  }

  def writeBytes(bytes: ByteVector): ByteVector = ByteVector.fromInt(bytes.length) ++ bytes

  def readBytes(data: ByteVector): (ByteVector, ByteVector) = {
    val (size, rem1) = readInt(data)
    rem1.splitAt(size)
  }

  def writeShortBytes(bytes: ByteVector): ByteVector = ByteVector.fromShort(bytes.length.toShort) ++ bytes

  def readShortBytes(data: ByteVector): (ByteVector, ByteVector) = {
    val (size, rem1) = readShort(data)
    rem1.splitAt(size)
  }

  def writeInet(a: InetSocketAddress): ByteVector = {
    val ip = a.getAddress.getAddress
    ByteVector(ip.length.toByte) ++ ByteVector(ip) ++ ByteVector.fromInt(a.getPort)
  }

  def readInet(data: ByteVector): (InetSocketAddress, ByteVector) = {
    val (ipSize, rem1) = data.splitAt(1)
    val (addrAsBytes, rem2) = rem1.splitAt(ipSize.toInt())
    val ip = InetAddress.getByAddress(addrAsBytes.toArray)
    val (port, rem3) = readInt(rem2)
    new InetSocketAddress(ip, port) -> rem3
  }

  def writeStringMap(m: Map[String, String]): ByteVector = {
    ByteVector.fromShort(m.size.toShort) ++
      m.foldLeft(ByteVector.empty)((acc, kv) => acc ++ writeString(kv._1) ++ writeString(kv._2))
  }

  def readStringMap(data: ByteVector): (Map[String, String], ByteVector) = {
    val (mapSize, rest) = readShort(data)

    def loop(itemsToRead: Int, acc: Map[String, String], rem: ByteVector): (Map[String, String], ByteVector) =
      itemsToRead match {
        case 0 => (acc, rem)
        case _ =>
          val (newKey, newRem) = readString(rem)
          val (newValue, newRem2) = readString(newRem)
          loop(itemsToRead - 1, acc + (newKey -> newValue), newRem2)
      }

    loop(mapSize, Map.empty, rest)
  }

  def writeStringMultiMap(m: Map[String, Seq[String]]): ByteVector = {
    ByteVector.fromShort(m.size.toShort) ++
      m.foldLeft(ByteVector.empty)((acc, kv) => acc ++ writeString(kv._1) ++ writeStringList(kv._2))
  }

  def readStringMultiMap(data: ByteVector): (Map[String, Seq[String]], ByteVector) = {
    val (mapSize, rem) = readShort(data)

    def loop(itemsToRead: Int, acc: Map[String, Seq[String]], rem: ByteVector): (Map[String, Seq[String]], ByteVector) =
      itemsToRead match {
        case 0 => (acc, rem)
        case _ =>
          val (newKey, newRem) = readString(rem)
          val (newValue, newRem2) = readStringList(newRem)
          loop(itemsToRead - 1, acc + (newKey -> newValue), newRem2)
      }

    loop(mapSize, Map.empty, rem)
  }

}
