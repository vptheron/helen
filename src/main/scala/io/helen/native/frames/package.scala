package io.helen.native

import akka.util.{ByteStringBuilder, ByteString}

package object frames {

  implicit private[frames] val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  private[frames] case class RawFrame(version: Byte,
                                      flags: Byte,
                                      stream: Byte,
                                      opsCode: Byte,
                                      body: ByteString)

  private[frames] def toBytes(frame: RawFrame): ByteString =
    new ByteStringBuilder()
      .putByte(frame.version)
      .putByte(frame.flags)
      .putByte(frame.stream)
      .putByte(frame.opsCode)
      .putInt(frame.body.length).append(frame.body)
      .result()

}