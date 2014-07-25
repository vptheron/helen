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

import akka.util.{ByteIterator, ByteString}

private[native] object Responses {

  import Body._

  private val VERSION: Byte = 0x12

  def fromBytes(data: ByteString): Response = {
    val dataIt = data.iterator
    val (version, flags, stream, opsCode, length) =
      (dataIt.getByte, dataIt.getByte, dataIt.getByte, dataIt.getByte, dataIt.getInt)

    opsCode match {
      case 0x00 => parseError(stream, dataIt)
      case 0x02 => Ready(stream)
      case 0x08 => parseResult(stream, dataIt)
      case 0x0C => parseEvent(stream, dataIt)
    }
  }

  private def parseError(stream: Byte, dataIterator: ByteIterator): Error = {
    val errorCode = dataIterator.getInt
    val errorMessage = readString(dataIterator)
    Error(stream, errorCode, errorMessage)
  }

  private def parseResult(stream: Byte, dataIterator: ByteIterator): Result = {
    val kind = dataIterator.getInt
    kind match {
      case 0x0001 => Void(stream)
      case 0x0002 => parseRows(stream, dataIterator)
      case 0x0003 => SetKeyspace(stream, readString(dataIterator))
      case 0x0004 => null
      case 0x0005 => SchemaChange(readString(dataIterator), readString(dataIterator), readString(dataIterator))
    }
  }

  private def parseRows(stream: Byte, dataIterator: ByteIterator): Rows = {
    val metadata = parseMetaData(dataIterator)
    val rowsCount = dataIterator.getInt

    val content = (0 until rowsCount) map { _ =>
      (0 until metadata.columnsCount) map { _ =>
        readBytes(dataIterator)
      }
    }

    Rows(stream, metadata, content)
  }

  private def parseMetaData(dataIterator: ByteIterator): ResultMetadata = {
    val flags = dataIterator.getInt
    val columnsCount = dataIterator.getInt

    val pagingStateOpt = if ((flags & 0x0002) == 1) Some(readBytes(dataIterator)) else None

    if ((flags & 0x0004) == 1) {
      ResultMetadata(columnsCount, pagingStateOpt, None, Nil)
    } else {

      val globalTableSpecOpt =
        if ((flags & 0x0001) == 1)
          Some((readString(dataIterator), readString(dataIterator)))
        else
          None

      val columnSpecs = (0 until columnsCount) map { _ =>
        val (ksNameOpt, tableNameOpt) =
          if (globalTableSpecOpt.isDefined)
            (None, None)
          else
            (Some(readString(dataIterator)), Some(readString(dataIterator)))

        val columnName = readString(dataIterator)
        readOption(dataIterator)
        ColumnSpec(ksNameOpt, tableNameOpt, columnName)
      }

      ResultMetadata(columnsCount, pagingStateOpt, globalTableSpecOpt, columnSpecs)
    }
  }

  case class ColumnSpec(keyspaceName: Option[String],
                        tableName: Option[String],
                        columnName: String)

  case class ResultMetadata(columnsCount: Int,
                            pagingStage: Option[ByteString],
                            globalTableSpec: Option[(String, String)],
                            columnSpecs: Seq[ColumnSpec])

  private def parseEvent(stream: Byte,
                         dataIterator: ByteIterator): Event = {
    val eventType = readString(dataIterator)
    eventType match {
      case "TOPOLOGY_CHANGE" =>
        TopologyChange(readString(dataIterator))
      case "STATUS_CHANGE" =>
        StatusChange(readString(dataIterator))
      case "SCHEMA_CHANGE" =>
        SchemaChange(readString(dataIterator), readString(dataIterator), readString(dataIterator))
    }
  }

  sealed trait Response

  case class Error(stream: Byte,
                   errorCode: Int,
                   errorMessage: String) extends Response

  case class Ready(stream: Byte) extends Response

  sealed trait Result extends Response

  case class Void(stream: Byte) extends Result

  case class Rows(stream: Byte, metadata: ResultMetadata, content: Seq[Seq[ByteString]]) extends Result

  case class SetKeyspace(stream: Byte, keyspace: String) extends Result

  // case class Prepared

  sealed trait Event extends Response

  case class TopologyChange(change: String) extends Event

  case class StatusChange(change: String) extends Event

  case class SchemaChange(change: String,
                          keyspace: String,
                          table: String) extends Event with Result

}

