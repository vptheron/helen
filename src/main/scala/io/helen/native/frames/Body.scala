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

object Body {

  implicit private val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

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

  def stringList(ss: Seq[String]): ByteString = {
    val builder = new ByteStringBuilder()
      .putShort(ss.length)
    ss.foreach(s => builder.append(string(s)))
    builder.result()
  }

  def bytes(b: ByteString): ByteString = {
    new ByteStringBuilder()
      .putInt(b.length)
      .append(b)
      .result()
  }

  def shortBytes(b: ByteString): ByteString = {
    new ByteStringBuilder()
      .putShort(b.length)
      .append(b)
      .result()
  }

  def stringMap(m: Map[String, String]): ByteString = {
    val builder = new ByteStringBuilder()
    builder.putShort(m.size)
    m.foreach(kv => builder.append(string(kv._1)).append(string(kv._2)))
    builder.result()
  }

  def stringMultiMap(m: Map[String, Seq[String]]): ByteString = {
    val builder = new ByteStringBuilder()
    builder.putShort(m.size)
    m.foreach(kv => builder.append(string(kv._1)).append(stringList(kv._2)))
    builder.result()
  }

}
