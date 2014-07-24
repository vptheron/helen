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

object Requests {

  def startup(stream: Byte): ByteString = {
    val body = Body.stringMap(Map("CQL_VERSION" -> "3.0.0"))
    toBytes(RawFrame(0x02, 0x00, stream, 0x01, body))
  }

  def register(stream: Byte): ByteString = {
    val body = Body.stringList(List("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE"))
    toBytes(RawFrame(0x02, 0x00, stream, 0x0B, body))
  }

  def query(stream: Byte,
            query: String,
            consistency: Short = 0x0001): ByteString = {
    val queryAsBytes = ByteString.fromString(query)
    val body = new ByteStringBuilder
    body.putInt(queryAsBytes.length).append(queryAsBytes)
      .putShort(consistency)
      .putByte(0x00)

    toBytes(RawFrame(0x02, 0x00, stream, 0x07, body.result()))
  }

}
