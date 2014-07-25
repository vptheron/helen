/*
 *      Copyright (C) 2013 Vincent Theron
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
package io.helen.native.frames

import akka.util.{ByteIterator, ByteStringBuilder, ByteString}

private[frames] object Body {

  def string(s: String): ByteString = {
    val sAsBytes = ByteString.fromString(s)
    new ByteStringBuilder()
      .putShort(sAsBytes.length)
      .append(sAsBytes)
      .result()
  }

  def readString(dataIterator: ByteIterator): String = {
    val buffer = new Array[Byte](dataIterator.getShort)
    dataIterator.getBytes(buffer)
    ByteString(buffer).utf8String
  }

  def longString(s: String): ByteString = {
    val sAsBytes = ByteString.fromString(s)
    new ByteStringBuilder()
      .putInt(sAsBytes.length)
      .append(sAsBytes)
      .result()
  }

  def readLongString(dataIterator: ByteIterator): String = {
    val buffer = new Array[Byte](dataIterator.getInt)
    dataIterator.getBytes(buffer)
    ByteString(buffer).utf8String
  }

  def stringList(ss: Seq[String]): ByteString = {
    val builder = new ByteStringBuilder()
      .putShort(ss.length)
    ss.foreach(s => builder.append(string(s)))
    builder.result()
  }

  def readStringList(dataIterator: ByteIterator): Seq[String] = {
    val listLength = dataIterator.getShort
    (0 until listLength).map(_ => readString(dataIterator))
  }

  def bytes(b: ByteString): ByteString = {
    new ByteStringBuilder()
      .putInt(b.length)
      .append(b)
      .result()
  }

  def readBytes(dataIterator: ByteIterator): Option[ByteString] = {
    val size = dataIterator.getInt
    if(size < 0)
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

  //FIXME need to find a good return type
  def readOption(dataIterator: ByteIterator) {
    val opt = dataIterator.getShort
    opt match {
      case 0x0000 =>
        val value = readString(dataIterator)

      case 0x0001 | 0x0002 | 0x0003 | 0x0004 |
           0x0005 | 0x0006 | 0x0007 | 0x0008 |
           0x0009 | 0x000A | 0x000B | 0x000C |
           0x000D | 0x000E | 0x000F | 0x0010 => List(opt)

      case 0x0020 =>
        val t = readOption(dataIterator)

      case 0x0021 =>
        val keyType = readOption(dataIterator)
        val valueType = readOption(dataIterator)

      case 0x0022 =>
        val t = readOption(dataIterator)
    }
  }

}
