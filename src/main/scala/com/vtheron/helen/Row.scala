package com.vtheron.helen

import java.util.{UUID, Date}
import java.nio.ByteBuffer
import java.net.InetAddress

trait Row {

  def containsColumn(name: String): Boolean

  def columnCount: Int

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

  def getIndexAsList[A](i: Int)(implicit m: Manifest[A]): List[A]

  def getColumnAsList[A](name: String)(implicit m: Manifest[A]): List[A]

  def getIndexAsLong(i: Int): Long

  def getColumnAsLong(name: String): Long

  def getIndexAsMap[K, V](i: Int)(implicit km: Manifest[K], vm: Manifest[V]): Map[K, V]

  def getColumnAsMap[K, V](name: String)(implicit km: Manifest[K], vm: Manifest[V]): Map[K, V]

  def getIndexAsSet[A](i: Int)(implicit m: Manifest[A]): Set[A]

  def getColumnAsSet[A](name: String)(implicit m: Manifest[A]): Set[A]

  def getIndexAsString(i: Int): String

  def getColumnAsString(name: String): String

  def getIndexAsUUID(i: Int): UUID

  def getColumnAsUUID(name: String): UUID

  def getIndexAsBigInt(i: Int): BigInt

  def getColumnAsBigInt(name: String): BigInt

//  def getIndexAs[A](index: Int)(implicit m: Manifest[A]): Try[A]
//
//  def getColumnAs[A](name: String)(implicit m: Manifest[A]): Try[A]

}
