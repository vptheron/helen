package io.helen.cql

import java.net.InetSocketAddress

import io.helen.cql.frames.types.ColumnType
import scodec.bits.ByteVector

package object frames {

  val MaxStream: Short = 32768.toShort

  case class Frame(stream: Short, message: Message) {

    require(stream >= -1 && stream <= MaxStream)
  }

  sealed trait Message

  // Requests
  sealed trait Request extends Message

  case object Startup extends Request

  case class AuthResponse(token: ByteVector) extends Request

  case object Options extends Request

  case class Query(query: String, parameters: QueryParameters = QueryParameters()) extends Request

  case class Prepare(query: String) extends Request

  case class Execute(preparedId: ByteVector, parameters: QueryParameters = QueryParameters()) extends Request

  case class Batch(queries: Seq[BatchQuery],
                   batchType: BatchType = LoggedBatch,
                   consistency: Consistency = One,
                   serialConsistency: Consistency = Serial,
                   defaultTimestamp: Option[Long] = None) extends Request {

    require(serialConsistency == Serial || serialConsistency == LocalSerial)
  }

  case class Register(topologyChange: Boolean = false,
                      statusChange: Boolean = false,
                      schemaChange: Boolean = false) extends Request

  // Request elements
  case class QueryParameters(consistency: Consistency = One,
                             values: Seq[ByteVector] = Nil,
                             valueNames: Seq[String] = Nil,
                             skipMetadata: Boolean = false,
                             pageSize: Option[Int] = None,
                             pagingState: Option[ByteVector] = None,
                             serialConsistency: Consistency = Serial,
                             defaultTimestamp: Option[Long] = None) {

    require(valueNames.isEmpty || valueNames.size == values.size)
    require(serialConsistency == Serial || serialConsistency == LocalSerial)
  }

  sealed trait BatchType

  case object LoggedBatch extends BatchType

  case object UnloggedBatch extends BatchType

  case object CounterBatch extends BatchType

  sealed trait BatchQuery

  case class PreparedBatchQuery(preparedId: ByteVector,
                                values: Seq[ByteVector],
                                valueNames: Seq[String]) extends BatchQuery {

    require(valueNames.isEmpty || valueNames.size == values.size)
  }

  case class UnpreparedBatchQuery(query: String,
                                  values: Seq[ByteVector] = Nil,
                                  valueNames: Seq[String] = Nil) extends BatchQuery {

    require(valueNames.isEmpty || valueNames.size == values.size)
  }

  // Responses
  sealed trait Response

  case class Error(code: Int, message: String) extends Response

  case object Ready extends Response

  case class Authenticate(authenticator: String) extends Response

  case class Supported(options: Map[String, Set[String]]) extends Response

  case class Result(data: ResultContent) extends Response

  sealed trait ResultContent

  case object Void extends ResultContent

  case class Rows(metadata: Metadata, content: Seq[Seq[ByteVector]]) extends ResultContent

  case class SetKeyspace(keyspace: String) extends ResultContent

  case class Prepared(id: ByteVector, metadata: Metadata) extends ResultContent

  case class Event(data: EventData) extends Response

  sealed trait EventData

  case class TopologyChange(newNode: Boolean, node: InetSocketAddress) extends EventData

  case class StatusChange(nodeUp: Boolean, node: InetSocketAddress) extends EventData

  case class SchemaChange(change: Change,
                          target: Target,
                          keyspace: String,
                          affectedObject: Option[String]) extends EventData with ResultContent

  case class AuthChallenge(token: ByteVector) extends Response

  case class AuthSuccess(token: ByteVector) extends Response

  // Response elements
  sealed trait Change

  case object Created extends Change

  case object Updated extends Change

  case object Dropped extends Change

  sealed trait Target

  case object Keyspace extends Target

  case object Table extends Target

  case object Type extends Target

  case class Metadata(columnsCount: Int,
                      pagingStage: Option[ByteVector],
                      globalTableSpec: Option[(String, String)],
                      columnSpecs: Seq[ColumnSpec])

  case class ColumnSpec(keyspaceName: Option[String],
                        tableName: Option[String],
                        columnName: String,
                        columnType: ColumnType)

  sealed trait Consistency

  case object Any extends Consistency

  case object One extends Consistency

  case object Two extends Consistency

  case object Three extends Consistency

  case object Quorum extends Consistency

  case object All extends Consistency

  case object LocalQuorum extends Consistency

  case object EachQuorum extends Consistency

  case object Serial extends Consistency

  case object LocalSerial extends Consistency

  case object LocalOne extends Consistency

}