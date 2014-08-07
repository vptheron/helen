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
package io.helen

import java.net.InetAddress
import java.util.UUID

import akka.util.ByteString
import org.joda.time.DateTime

trait Row {

  def getColumnsMetadata: ColumnsMetadata

  def isNull(i: Int): Boolean

  def isNull(name: String): Boolean

  def getAscii(i: Int): String

  def getAscii(name: String): String

  def getBigInt(i: Int): Long

  def getBigInt(name: String): Long

  def getBlob(i: Int): ByteString

  def getBlob(name: String): ByteString

  def getBoolean(i: Int): Boolean

  def getBoolean(name: String): Boolean

  def getCounter(i: Int): Long

  def getCounter(name: String): Long

  def getDecimal(i: Int): BigDecimal

  def getDecimal(name: String): BigDecimal

  def getDouble(i: Int): Double

  def getDouble(name: String): Double

  def getFloat(i: Int): Float

  def getFloat(name: String): Float

  def getInet(i: Int): InetAddress

  def getInet(name: String): InetAddress

  def getInt(i: Int): Int

  def getInt(name: String): Int

  def getText(i: Int): String

  def getText(name: String): String

  def getTimestamp(i: Int): DateTime

  def getTimestamp(name: String): DateTime

  def getUUID(i: Int): UUID

  def getUUID(name: String): UUID

  def getVarchar(i: Int): String

  def getVarchar(name: String): String

  def getVarint(i: Int): BigInt

  def getVarint(name: String): BigInt

  def getTimeUUID(i: Int): UUID

  def getTimeUUID(name: String): UUID

  def getList[A](i: Int): List[A]

  def getList[A](name: String): List[A]

  def getSet[A](i: Int): Set[A]

  def getSet[A](name: String): Set[A]

  def getMap[K, V](i: Int): Map[K, V]

  def getMap[K, V](name: String): Map[K, V]

  def getRawBytes(i: Int): ByteString

  def getRawBytes(name: String): ByteString

}
