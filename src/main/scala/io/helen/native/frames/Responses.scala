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

import akka.util.ByteString

object Responses {

  import Body._

  def fromData(data: ByteString): Response = {
    val dataIt = data.iterator
    val version = dataIt.getByte
    val flags = dataIt.getByte
    val stream = dataIt.getByte
    val opsCode = dataIt.getByte
    val length = dataIt.getInt

    opsCode match {
      case 0x00 =>
        val errorCode = dataIt.getInt
        val errorMessage = readString(dataIt)
        Error(stream, errorCode, errorMessage)

      case 0x02 => Ready(stream)

      case 0x0C =>
        val eventType = readString(dataIt)
        eventType  match {
          case "TOPOLOGY_CHANGE" =>
            TopologyChange(readString(dataIt))
          case "STATUS_CHANGE" =>
            StatusChange(readString(dataIt))
          case "SCHEMA_CHANGE" =>
            SchemaChange(readString(dataIt), readString(dataIt), readString(dataIt))
        }
    }
  }

  sealed trait Response

  case class Error(stream: Byte,
                   errorCode: Int,
                   errorMessage: String) extends Response

  case class Ready(stream: Byte) extends Response

  sealed trait Event extends Response

  case class TopologyChange(change: String) extends Event

  case class StatusChange(change: String) extends Event

  case class SchemaChange(change: String, keyspace: String, table: String) extends Event

}

