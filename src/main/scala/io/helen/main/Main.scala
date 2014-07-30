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
package io.helen.main

import java.util.UUID

import akka.actor.ActorSystem
import akka.util.{ByteStringBuilder, ByteString}
import io.helen.cql.Requests.{UnpreparedBatchQuery, LoggedBatch, QueryParameters}
import io.helen.cql.Responses.Prepared
import io.helen.cql.{Body, CqlClient, ActorBackedCqlClient, Requests}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  private val timeout = Duration("5 seconds")


  def main(args: Array[String]) {

    val system = ActorSystem("helen-system")

    val client = new ActorBackedCqlClient("localhost", 9042)(system)

    println(Await.result(client.send(Requests.Startup), timeout))

    //    setupKeyspaceTable(client)

    //    useOptions(client)

    //    insertWithPrepareExecute(client)

//    selectWithValues(client)
//    selectWithPrepare(client)

//    batchInsert(client)

    selectAll(client)

    client.close()
    system.shutdown()
  }

  private def useOptions(client: CqlClient) {
    println(Await.result(client.send(Requests.Options), timeout))
  }

  private def setupKeyspaceTable(client: CqlClient) {
    println(Await.result(
      client.send(
        Requests.Query("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}")),
      timeout))

    println(Await.result(
      client.send(
        Requests.Query("CREATE TABLE demodb.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob)")),
      timeout))
  }

  private def insertWithPrepareExecute(client: CqlClient) {
    val prepared = Await.result(
      client.send(
        Requests.Prepare("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)")),
      timeout).asInstanceOf[Prepared]

    println(prepared)

    val boundValues = List(
      uuid(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")),
      ByteString.fromString("La Petite Tonkinoise"),
      ByteString.fromString("Bye Bye Blackbird"),
      ByteString.fromString("Josephine Baker"),
      set(Set(ByteString.fromString("jazz"), ByteString.fromString("2013")))
    )
    val execute = Requests.Execute(prepared.id, QueryParameters(values = boundValues))

    println(Await.result(client.send(execute), timeout))
  }

  private def selectWithValues(client: CqlClient) {
    val boundValues = List(uuid(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")))

    println(
      Await.result(
        client.send(Requests.Query("SELECT * FROM demodb.songs WHERE id = ?", QueryParameters(values = boundValues))),
        timeout)
    )
  }

  private def batchInsert(client: CqlClient) {
    val queries = List(
      UnpreparedBatchQuery("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (999716f7-2e54-4715-9f00-91dcbea6cf50, 'hello', 'world', 'author', {'rock', '2014'})", Nil),
      UnpreparedBatchQuery("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (888716f7-2e54-4715-9f00-91dcbea6cf50, 'hello1', 'world1', 'author1', {'rock', '2014'})", Nil)
    )
    println(
      Await.result(
        client.send(Requests.Batch(queries)),
        timeout)
    )
  }

  private def selectAll(client: CqlClient) {
    println(
      Await.result(
        client.send(Requests.Query("SELECT * FROM demodb.songs")),
        timeout)
    )
  }

  private def selectWithPrepare(client: CqlClient) {
    val prepared = Await.result(
      client.send(
        Requests.Prepare("SELECT * FROM demodb.songs WHERE id = ?")),
      timeout).asInstanceOf[Prepared]

    println(prepared)

    val boundValues = List(
      uuid(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"))
    )

    val execute = Requests.Execute(prepared.id, QueryParameters(values = boundValues))

    println(Await.result(client.send(execute), timeout))
  }

  import Body.byteOrder

  private def uuid(id: UUID): ByteString = {
    new ByteStringBuilder()
      .putLong(id.getMostSignificantBits)
      .putLong(id.getLeastSignificantBits)
      .result()
  }

  def set(s: Set[ByteString]): ByteString = {
    val builder = new ByteStringBuilder()
      .putShort(s.size)

    s.foreach(v => builder.append(Body.shortBytes(v)))
    builder.result()
  }

}
