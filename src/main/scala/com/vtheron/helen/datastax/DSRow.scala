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
package com.vtheron.helen.datastax

import com.datastax.driver.core.{Row => JRow}
import java.util.{UUID, Date}
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import java.net.InetAddress
import com.vtheron.helen.Row
import scala.reflect.{ClassTag, classTag}

private[datastax] class DSRow(jRow: JRow) extends Row {

  def isIndexNull(i: Int): Boolean = jRow.isNull(i)

  def isColumnNull(name: String): Boolean = jRow.isNull(name)

  def getIndexAsBool(i: Int): Boolean = jRow.getBool(i)

  def getColumnAsBool(name: String): Boolean = jRow.getBool(name)

  def getIndexAsBytes(i: Int): ByteBuffer = jRow.getBytes(i)

  def getColumnAsBytes(name: String): ByteBuffer = jRow.getBytes(name)

  def getIndexAsDate(i: Int): Date = jRow.getDate(i)

  def getColumnAsDate(name: String): Date = jRow.getDate(name)

  def getIndexAsBigDecimal(i: Int): BigDecimal = BigDecimal(jRow.getDecimal(i))

  def getColumnAsBigDecimal(name: String): BigDecimal = BigDecimal(jRow.getDecimal(name))

  def getIndexAsDouble(i: Int): Double = jRow.getDouble(i)

  def getColumnAsDouble(name: String): Double = jRow.getDouble(name)

  def getIndexAsFloat(i: Int): Float = jRow.getFloat(i)

  def getColumnAsFloat(name: String): Float = jRow.getFloat(name)

  def getIndexAsInetAddress(i: Int): InetAddress = jRow.getInet(i)

  def getColumnAsInetAddress(name: String): InetAddress = jRow.getInet(name)

  def getIndexAsInt(i: Int): Int = jRow.getInt(i)

  def getColumnAsInt(name: String): Int = jRow.getInt(name)

  def getIndexAsList[A: ClassTag](i: Int): List[A] =
    jRow.getList(i, classTag[A].runtimeClass.asInstanceOf[Class[A]]).toList

  def getColumnAsList[A: ClassTag](name: String): List[A] =
    jRow.getList(name, classTag[A].runtimeClass.asInstanceOf[Class[A]]).toList

  def getIndexAsLong(i: Int): Long = jRow.getLong(i)

  def getColumnAsLong(name: String): Long = jRow.getLong(name)

  def getIndexAsMap[K: ClassTag, V: ClassTag](i: Int): Map[K, V] =
    jRow.getMap(i, classTag[K].runtimeClass.asInstanceOf[Class[K]], classTag[V].runtimeClass.asInstanceOf[Class[V]]).toMap

  def getColumnAsMap[K: ClassTag, V: ClassTag](name: String): Map[K, V] =
    jRow.getMap(name, classTag[K].runtimeClass.asInstanceOf[Class[K]], classTag[V].runtimeClass.asInstanceOf[Class[V]]).toMap

  def getIndexAsSet[A: ClassTag](i: Int): Set[A] =
    jRow.getSet(i, classTag[A].runtimeClass.asInstanceOf[Class[A]]).toSet

  def getColumnAsSet[A: ClassTag](name: String): Set[A] =
    jRow.getSet(name, classTag[A].runtimeClass.asInstanceOf[Class[A]]).toSet

  def getIndexAsString(i: Int): String = jRow.getString(i)

  def getColumnAsString(name: String): String = jRow.getString(name)

  def getIndexAsUUID(i: Int): UUID = jRow.getUUID(i)

  def getColumnAsUUID(name: String): UUID = jRow.getUUID(name)

  def getIndexAsBigInt(i: Int): BigInt = BigInt(jRow.getVarint(i))

  def getColumnAsBigInt(name: String): BigInt = BigInt(jRow.getVarint(name))

}
