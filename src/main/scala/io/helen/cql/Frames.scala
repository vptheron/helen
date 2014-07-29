/*
 *      Copyright (C) 2014 Vincent Theron
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
package io.helen.cql

import akka.util.{ByteIterator, ByteString, ByteStringBuilder}
import io.helen.cql.Requests._
import io.helen.cql.Responses._

private[cql] object Frames {

  import Body._

  def fromRequest(stream: Byte, req: Request): ByteString = {
    val (opsCode, body) =
      req match {
        case Startup => (0x01, Body.stringMap(Map("CQL_VERSION" -> "3.0.0")))
        case AuthResponse(token) => (0x0F, Body.bytes(token))
        case Options => (0x05, ByteString())
        case Query(q, params) => (0x07, serializeQuery(q, params))
        case Prepare(query) => (0x09, Body.longString(query))
        case Execute(id, params) => (0x0A, serializeExecute(id, params))
        case Batch(batchType, queries, consistency) => (0x0D, serializeBatch(batchType, queries, consistency))
        case Register(topology, status, schema) => (0x0B, serializeRegister(topology, status, schema))
      }

    new ByteStringBuilder().putByte(0x02).putByte(0x00)
      .putByte(stream).putByte(opsCode.toByte).putInt(body.length).append(body)
      .result()
  }

  private def serializeQuery(query: String, parameters: QueryParameters): ByteString =
    new ByteStringBuilder()
      .append(longString(query))
      .append(serializeQueryParameters(parameters))
      .result()

  private def serializeExecute(id: ByteString, parameters: QueryParameters): ByteString =
    new ByteStringBuilder()
      .append(shortBytes(id))
      .append(serializeQueryParameters(parameters))
      .result()


  private def serializeQueryParameters(params: QueryParameters): ByteString = {
    val builder = new ByteStringBuilder()

    builder.putShort(serializeConsistency(params.consistency))

    val flags =
      (if (params.values.nonEmpty) 0x01 else 0x00) |
        (if (params.skipMetadata) 0x02 else 0x00) |
        (if (params.pageSize.isDefined) 0x04 else 0x00) |
        (if (params.pagingState.isDefined) 0x08 else 0x00) |
        (if (params.serialConsistency.isDefined) 0x10 else 0x00)
    builder.putByte(flags.toByte)

    if (params.values.nonEmpty) {
      builder.putShort(params.values.size)
      params.values.foreach(v => builder.append(bytes(v)))
    }

    params.pageSize.foreach(size => builder.putInt(size))
    params.pagingState.foreach(st => builder.append(bytes(st)))
    params.serialConsistency.foreach(c => builder.putShort(serializeConsistency(c)))

    builder.result()
  }

  private def serializeConsistency(consistency: Consistency): Short = consistency match {
    case Any => 0x0000
    case One => 0x0001
    case Two => 0x0002
    case Three => 0x0003
    case Quorum => 0x0004
    case All => 0x0005
    case LocalQuorum => 0x0006
    case EachQuorum => 0x0007
    case Serial => 0x0008
    case LocalSerial => 0x0009
    case LocalOne => 0x000A
  }

  private def serializeBatch(batchType: BatchType, queries: Seq[BatchQuery], consistency: Consistency): ByteString = {
    val body = new ByteStringBuilder()

    val typeByte: Byte = batchType match {
      case LoggedBatch => 0
      case UnloggedBatch => 1
      case CounterBatch => 2
    }

    body.putByte(typeByte)
    body.putShort(queries.size)
    queries.foreach(q => body.append(serializeBatchQuery(q)))
    body.putShort(serializeConsistency(consistency))
    body.result()
  }

  private def serializeBatchQuery(query: BatchQuery): ByteString = {
    val builder = new ByteStringBuilder()

    query match {
      case UnpreparedBatchQuery(q, values) =>
        builder.putByte(0).append(longString(q)).putShort(values.size)
        values.foreach(v => builder.append(bytes(v)))
      case PreparedBatchQuery(id, values) =>
        builder.putByte(1).append(shortBytes(id)).putShort(values.size)
        values.foreach(v => builder.append(bytes(v)))
    }

    builder.result()
  }

  private def serializeRegister(topology: Boolean, status: Boolean, schema: Boolean): ByteString = {
    val events =
      (if (topology) List("TOPOLOGY_CHANGE") else Nil) ++
        (if (status) List("STATUS_CHANGE") else Nil) ++
        (if (schema) List("SCHEMA_CHANGE") else Nil)
    Body.stringList(events)
  }


  def fromBytes(data: ByteString): (Byte, Response) = {
    val dataIt = data.iterator
    val (version, flags, stream, opsCode, length) =
      (dataIt.getByte, dataIt.getByte, dataIt.getByte, dataIt.getByte, dataIt.getInt)

    val response = opsCode match {
      case 0x00 => Error(dataIt.getInt, readString(dataIt))
      case 0x02 => Ready
      case 0x03 => Authenticate(readString(dataIt))
      case 0x06 => Supported(readStringMultiMap(dataIt))
      case 0x08 => parseResult(dataIt)
      case 0x0C => parseEvent(dataIt)
      case 0x0E => AuthChallenge(readBytes(dataIt))
      case 0x10 => AuthSuccess(readBytes(dataIt))
    }

    (stream, response)
  }

  private def parseResult(dataIterator: ByteIterator): Result = dataIterator.getInt match {
    case 0x0001 => Void
    case 0x0002 => parseRows(dataIterator)
    case 0x0003 => SetKeyspace(readString(dataIterator))
    case 0x0004 => Prepared(readShortBytes(dataIterator), parseMetaData(dataIterator))
    case 0x0005 => parseSchemaChange(dataIterator)
  }

  private def parseRows(dataIterator: ByteIterator): Rows = {
    val metadata = parseMetaData(dataIterator)
    val rowsCount = dataIterator.getInt
    val content = (0 until rowsCount) map {
      i =>
        (0 until metadata.columnsCount) map {
          j =>
            readBytes(dataIterator)
        }
    }

    Rows(metadata, content)
  }

  private def parseMetaData(dataIterator: ByteIterator): Metadata = {
    val flags = dataIterator.getInt
    val columnsCount = dataIterator.getInt

    val pagingStateOpt = if ((flags & (1 << 1)) != 0) readBytes(dataIterator) else None

    if ((flags & (1 << 2)) != 0) {
      Metadata(columnsCount, pagingStateOpt, None, Nil)
    } else {

      val globalTableSpecOpt =
        if ((flags & (1 << 0)) != 0)
          Some((readString(dataIterator), readString(dataIterator)))
        else
          None

      val columnSpecs = (0 until columnsCount) map {
        _ =>
          val (ksNameOpt, tableNameOpt) =
            if (globalTableSpecOpt.isDefined)
              (None, None)
            else
              (Some(readString(dataIterator)), Some(readString(dataIterator)))

          val columnName = readString(dataIterator)
          readOption(dataIterator)
          ColumnSpec(ksNameOpt, tableNameOpt, columnName)
      }

      Metadata(columnsCount, pagingStateOpt, globalTableSpecOpt, columnSpecs)
    }
  }

  private def parseEvent(dataIterator: ByteIterator): Event = readString(dataIterator) match {
    case "TOPOLOGY_CHANGE" => TopologyChange(readString(dataIterator) == "NEW_NODE", readAddress(dataIterator))
    case "STATUS_CHANGE" => StatusChange(readString(dataIterator) == "UP", readAddress(dataIterator))
    case "SCHEMA_CHANGE" => parseSchemaChange(dataIterator)
  }

  private def parseSchemaChange(dataIterator: ByteIterator): SchemaChange = {
    val change = readString(dataIterator) match {
      case "CREATED" => Created
      case "UPDATED" => Updated
      case "DROPPED" => Dropped
    }
    SchemaChange(change, readString(dataIterator), readString(dataIterator))
  }

}
