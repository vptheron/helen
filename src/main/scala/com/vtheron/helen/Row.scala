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
package com.vtheron.helen

import java.util.{UUID, Date}
import java.nio.ByteBuffer
import java.net.InetAddress
import scala.reflect.ClassTag

trait Row {

  //  def containsColumn(name: String): Boolean
  //
  //  def columnCount: Int

  def isIndexNull(i: Int): Boolean

  def isColumnNull(name: String): Boolean

  def getIndexAsBool(i: Int): Boolean

  def getColumnAsBool(name: String): Boolean

  def getIndexAsBytes(i: Int): ByteBuffer

  def getColumnAsBytes(name: String): ByteBuffer

  def getIndexAsDate(i: Int): Date

  def getColumnAsDate(name: String): Date

  def getIndexAsBigDecimal(i: Int): BigDecimal

  def getColumnAsBigDecimal(name: String): BigDecimal

  def getIndexAsDouble(i: Int): Double

  def getColumnAsDouble(name: String): Double

  def getIndexAsFloat(i: Int): Float

  def getColumnAsFloat(name: String): Float

  def getIndexAsInetAddress(i: Int): InetAddress

  def getColumnAsInetAddress(name: String): InetAddress

  def getIndexAsInt(i: Int): Int

  def getColumnAsInt(name: String): Int

  def getIndexAsList[A: ClassTag](i: Int): List[A]

  def getColumnAsList[A: ClassTag](name: String): List[A]

  def getIndexAsLong(i: Int): Long

  def getColumnAsLong(name: String): Long

  def getIndexAsMap[K: ClassTag, V: ClassTag](i: Int): Map[K, V]

  def getColumnAsMap[K: ClassTag, V: ClassTag](name: String): Map[K, V]

  def getIndexAsSet[A: ClassTag](i: Int): Set[A]

  def getColumnAsSet[A: ClassTag](name: String): Set[A]

  def getIndexAsString(i: Int): String

  def getColumnAsString(name: String): String

  def getIndexAsUUID(i: Int): UUID

  def getColumnAsUUID(name: String): UUID

  def getIndexAsBigInt(i: Int): BigInt

  def getColumnAsBigInt(name: String): BigInt

}
