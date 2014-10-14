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
package io.helen.cql.frames

object types {

  sealed trait ColumnType

  case class CustomType(value: String) extends ColumnType

  case object AsciiType extends ColumnType

  case object BigIntType extends ColumnType

  case object BlobType extends ColumnType

  case object BooleanType extends ColumnType

  case object CounterType extends ColumnType

  case object DecimalType extends ColumnType

  case object DoubleType extends ColumnType

  case object FloatType extends ColumnType

  case object IntType extends ColumnType

  case object TimestampType extends ColumnType

  case object UuidType extends ColumnType

  case object VarcharType extends ColumnType

  case object VarintType extends ColumnType

  case object TimeuuidType extends ColumnType

  case object InetType extends ColumnType

  case class ListType(c: ColumnType) extends ColumnType

  case class MapType(keyType: ColumnType, valueType: ColumnType) extends ColumnType

  case class SetType(c: ColumnType) extends ColumnType

  case class UdtType(keyspace: String, name: String, fields: Seq[(String, ColumnType)]) extends ColumnType

  case class TupleType(types: Seq[ColumnType]) extends ColumnType

}
