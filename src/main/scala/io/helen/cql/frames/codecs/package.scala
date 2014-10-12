package io.helen.cql.frames

import scodec.bits.ByteVector

import scala.util.Try

package object codecs {

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def encode(f: Frame): ByteVector = ???

  def decode(data: ByteVector): Try[(Frame, ByteVector)] = ???


}
