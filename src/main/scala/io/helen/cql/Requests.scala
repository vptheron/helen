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

import akka.util.ByteString

object Requests {

  sealed trait Request

  case object Startup extends Request

  case class AuthResponse(token: ByteString) extends Request

  case object Options extends Request

  case class Query(query: String,
                   parameters: QueryParameters = QueryParameters()) extends Request

  case class Prepare(query: String) extends Request

  case class Execute(preparedId: ByteString,
                     parameters: QueryParameters = QueryParameters()) extends Request

  case class Batch(batchType: BatchType,
                   queries: Seq[BatchQuery],
                   consistency: Consistency = One) extends Request

  case class Register(topologyChange: Boolean = false,
                      statusChange: Boolean = false,
                      schemaChange: Boolean = false) extends Request

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

  case class QueryParameters(consistency: Consistency = One,
                             values: Seq[ByteString] = Nil,
                             skipMetadata: Boolean = false,
                             pageSize: Option[Int] = None,
                             pagingState: Option[ByteString] = None,
                             serialConsistency: Option[Consistency] = None)

  sealed trait BatchType

  case object LoggedBatch extends BatchType

  case object UnloggedBatch extends BatchType

  case object CounterBatch extends BatchType

  sealed trait BatchQuery

  case class PreparedBatchQuery(preparedId: ByteString,
                                values: Seq[ByteString]) extends BatchQuery

  case class UnpreparedBatchQuery(query: String,
                                  values: Seq[ByteString]) extends BatchQuery

}
