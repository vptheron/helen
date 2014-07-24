package io.helen.native

import akka.util.{ByteStringBuilder, ByteString}

package object frames {

  implicit private[frames] val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  case class RawFrame(version: Byte,
                      flags: Byte,
                      stream: Byte,
                      opsCode: Byte,
                      body: ByteString)

  def toBytes(frame: RawFrame): ByteString =
    new ByteStringBuilder()
      .putByte(frame.version)
      .putByte(frame.flags)
      .putByte(frame.stream)
      .putByte(frame.opsCode)
      .putInt(frame.body.length).append(frame.body)
      .result()

  val requestVersion: Byte = 0x02
  val responseVersion: Byte = 0x12

}
