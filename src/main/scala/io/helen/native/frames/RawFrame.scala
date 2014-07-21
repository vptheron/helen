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

import akka.util.{ByteStringBuilder, ByteString}

case class RawFrame(version: Byte,
                    flags: Byte,
                    stream: Byte,
                    opsCode: Byte,
                    body: ByteString) {

  def toData: ByteString = {
    implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

    new ByteStringBuilder()
      .putByte(version)
      .putByte(flags)
      .putByte(stream)
      .putByte(opsCode)
      .putInt(body.length).append(body)
      .result()
  }

}

object RawFrame {
  val requestVersion: Byte = 0x02
  val responseVersion: Byte = 0x12
}