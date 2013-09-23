package com.vtheron.helen.datastax

import com.datastax.driver.core.{Row => JRow}
import java.util.{UUID, Date}
import java.nio.ByteBuffer
import scala.collection.JavaConversions._
import java.net.InetAddress
import com.vtheron.helen.Row
import scala.util.Try

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

  def getIndexAsList[A](i: Int)(implicit m: Manifest[A]): List[A] =
    jRow.getList(i, m.runtimeClass.asInstanceOf[Class[A]]).toList

  def getColumnAsList[A](name: String)(implicit m: Manifest[A]): List[A] =
    jRow.getList(name, m.runtimeClass.asInstanceOf[Class[A]]).toList

  def getIndexAsLong(i: Int): Long = jRow.getLong(i)

  def getColumnAsLong(name: String): Long = jRow.getLong(name)

  def getIndexAsMap[K, V](i: Int)(implicit km: Manifest[K], vm: Manifest[V]): Map[K, V] =
    jRow.getMap(i, km.runtimeClass.asInstanceOf[Class[K]], vm.runtimeClass.asInstanceOf[Class[V]]).toMap

  def getColumnAsMap[K, V](name: String)(implicit km: Manifest[K], vm: Manifest[V]): Map[K, V] =
    jRow.getMap(name, km.runtimeClass.asInstanceOf[Class[K]], vm.runtimeClass.asInstanceOf[Class[V]]).toMap

  def getIndexAsSet[A](i: Int)(implicit m: Manifest[A]): Set[A] =
    jRow.getSet(i, m.runtimeClass.asInstanceOf[Class[A]]).toSet

  def getColumnAsSet[A](name: String)(implicit m: Manifest[A]): Set[A] =
    jRow.getSet(name, m.runtimeClass.asInstanceOf[Class[A]]).toSet

  def getIndexAsString(i: Int): String = jRow.getString(i)

  def getColumnAsString(name: String): String = jRow.getString(name)

  def getIndexAsUUID(i: Int): UUID = jRow.getUUID(i)

  def getColumnAsUUID(name: String): UUID = jRow.getUUID(name)

  def getIndexAsBigInt(i: Int): BigInt = BigInt(jRow.getVarint(i))

  def getColumnAsBigInt(name: String): BigInt = BigInt(jRow.getVarint(name))

}
