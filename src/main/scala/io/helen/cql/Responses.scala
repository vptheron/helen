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

import java.net.InetSocketAddress

import akka.util.ByteString

object Responses {

  sealed trait Response

  case class Error(errorCode: Int,
                   errorMessage: String) extends Response

  case object Ready extends Response

  case class Authenticate(authenticator: String) extends Response

  case class Supported(options: Map[String, Seq[String]]) extends Response

  sealed trait Result extends Response

  case object Void extends Result

  case class Rows(metadata: Metadata,
                  content: Seq[Seq[Option[ByteString]]]) extends Result

  case class SetKeyspace(keyspace: String) extends Result

  case class Prepared(id: ByteString,
                      metadata: Metadata) extends Result

  sealed trait Event extends Response

  case class TopologyChange(nodeAdded: Boolean,
                            node: InetSocketAddress) extends Event

  case class StatusChange(nodeUp: Boolean,
                           node: InetSocketAddress) extends Event

  case class SchemaChange(change: Change,
                          keyspace: String,
                          table: String) extends Event with Result

  case class AuthChallenge(token: Option[ByteString]) extends Response

  case class AuthSuccess(token: Option[ByteString]) extends Response

  case class ColumnSpec(keyspaceName: Option[String],
                        tableName: Option[String],
                        columnName: String)

  case class Metadata(columnsCount: Int,
                      pagingStage: Option[ByteString],
                      globalTableSpec: Option[(String, String)],
                      columnSpecs: Seq[ColumnSpec])

  sealed trait Change

  case object Created extends Change

  case object Updated extends Change

  case object Dropped extends Change


}

