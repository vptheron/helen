package io.helen.cql.frames

import scodec.bits.ByteVector

package object codecs {

  import io.helen.cql.frames.codecs.Body._

  private val RequestVersion: Byte = 0x03.toByte
  private val ResponseVersion: Byte = 0x83.toByte

  def serialize(f: Frame): ByteVector = {
    val (opsCode, serializedMsg) = serializeMessage(f.message)
    val version = if (f.message.isInstanceOf[Request]) RequestVersion else ResponseVersion

    ByteVector(version, 0x00.toByte) ++ writeShort(f.stream) ++
      ByteVector(opsCode) ++ writeInt(serializedMsg.length) ++ serializedMsg
  }

  private def serializeMessage(m: Message): (Byte, ByteVector) = m match {
    // Requests
    case Startup => (0x01, writeStringMap(Map("CQL_VERSION" -> "3.0.0")))
    case AuthResponse(token) => (0x0F, writeBytes(token))
    case Options => (0x05, ByteVector.empty)
    case q: Query => (0x07, serializeQuery(q))
    case Prepare(q) => (0x09, writeLongString(q))
    case e: Execute => (0x0A, serializeExecute(e))
    case b: Batch => (0x0D, serializeBatch(b))
    case r: Register => (0x0B, serializeRegister(r))
    // Responses
    case Error(code, msg) => (0x00, writeInt(code) ++ writeString(msg))
    case Ready => (0x02, ByteVector.empty)
    case Authenticate(authenticator) => (0x03, writeString(authenticator))
    case Supported(options) => (0x06, writeStringMultiMap(options))
    case Result(data) => (0x08, ByteVector.empty) //FIXME
    case Event(event) => (0x0C, serializeEvent(event))
    case AuthChallenge(token) => (0x0E, writeBytes(token))
    case AuthSuccess(token) => (0x10, writeBytes(token))
  }

  private def serializeQuery(query: Query): ByteVector =
    writeLongString(query.query) ++ serializeQueryParameters(query.parameters)

  private def serializeQueryParameters(params: QueryParameters): ByteVector = {
    val flags =
      (if (params.values.nonEmpty) 0x01 else 0x00) |
        (if (params.skipMetadata) 0x02 else 0x00) |
        (if (params.pageSize.isDefined) 0x04 else 0x00) |
        (if (params.pagingState.isDefined) 0x08 else 0x00) |
        (if (params.serialConsistency.isDefined) 0x10 else 0x00) |
        (if (params.defaultTimestamp.isDefined) 0x20 else 0x00) |
        (if (params.valueNames.nonEmpty) 0x40 else 0x00)

    val values =
      if (params.values.nonEmpty) {
        val serializedValues =
          if (params.valueNames.nonEmpty) {
            params.values.zip(params.valueNames).map(vn => writeString(vn._2) ++ writeBytes(vn._1))
          } else {
            params.values.map(v => writeBytes(v))
          }

        writeShort(params.values.size.toShort) ++ serializedValues.foldLeft(ByteVector.empty)(_ ++ _)

      } else {
        ByteVector.empty
      }

    serializeConsistency(params.consistency) ++
      ByteVector(flags.toByte) ++
      values ++
      params.pageSize.fold(ByteVector.empty)(writeInt) ++
      params.pagingState.getOrElse(ByteVector.empty) ++
      params.serialConsistency.fold(ByteVector.empty)(serializeConsistency) ++
      params.defaultTimestamp.fold(ByteVector.empty)(writeLong)
  }

  private def serializeConsistency(consistency: Consistency): ByteVector = {
    val consistencyCode = consistency match {
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
    writeShort(consistencyCode.toShort)
  }

  private def serializeExecute(execute: Execute): ByteVector =
    writeShortBytes(execute.preparedId) ++ serializeQueryParameters(execute.parameters)

  private def serializeBatch(batch: Batch): ByteVector = {
    val typeByte: Byte = batch.batchType match {
      case LoggedBatch => 0
      case UnloggedBatch => 1
      case CounterBatch => 2
    }
    val flags =
      (if (batch.serialConsistency.isDefined) 0x10 else 0x00) |
        (if (batch.defaultTimestamp.isDefined) 0x20 else 0x00) |
        (if (batch.withNames) 0x40 else 0x00)

    ByteVector(typeByte) ++
      writeShort(batch.queries.size.toShort) ++
      batch.queries.map(serializeBatchQuery).foldLeft(ByteVector.empty)(_ ++ _) ++
      serializeConsistency(batch.consistency) ++
      ByteVector(flags) ++
      batch.serialConsistency.fold(ByteVector.empty)(serializeConsistency) ++
      batch.defaultTimestamp.fold(ByteVector.empty)(writeLong)
  }

  private def serializeBatchQuery(query: BatchQuery): ByteVector = {
    val kindAndId = query match {
      case UnpreparedBatchQuery(q, _, _) => ByteVector(0.toByte) ++ writeLongString(q)
      case PreparedBatchQuery(id, _, _) => ByteVector(1.toByte) ++ writeShortBytes(id)
    }

    val serializedValues =
      if (query.valueNames.nonEmpty) {
        query.values.zip(query.valueNames).map(vn => writeString(vn._2) ++ writeBytes(vn._1))
      } else {
        query.values.map(v => writeBytes(v))
      }

    kindAndId ++
      writeShort(query.values.size.toShort) ++
      serializedValues.foldLeft(ByteVector.empty)(_ ++ _)
  }

  private def serializeRegister(register: Register): ByteVector = {
    val events =
      (if (register.topologyChange) List("TOPOLOGY_CHANGE") else Nil) ++
        (if (register.statusChange) List("STATUS_CHANGE") else Nil) ++
        (if (register.schemaChange) List("SCHEMA_CHANGE") else Nil)
    writeStringList(events)
  }

  private def serializeEvent(event: EventData): ByteVector = event match {
    case TopologyChange(newNode, node) =>
      writeString("TOPOLOGY_CHANGE") ++ writeString(if(newNode) "NEW_NODE" else "REMOVED_NODE") ++ writeInet(node)
    case StatusChange(nodeUp, node) =>
      writeString("STATUS_CHANGE") ++ writeString(if(nodeUp) "UP" else "DOWN") ++ writeInet(node)
    case s: SchemaChange => writeString("SCHEMA_CHANGE") ++ serializeSchemaChange(s)
  }

  private def serializeSchemaChange(s: SchemaChange): ByteVector = {
    ???
  }

  def deserialize(data: ByteVector): Frame = ???


}
