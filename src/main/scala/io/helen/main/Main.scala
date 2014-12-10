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

import java.net.InetAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.util.ByteString
import io.helen.cql.Codecs.Implicits._
import io.helen.cql.Codecs._
import io.helen.cql.Requests
import io.helen.cql.Requests.{QueryParameters, Request, UnpreparedBatchQuery}
import io.helen.cql.Responses.{Prepared, Response, Rows}
import io.helen.cql.network.{ActorBackedCqlClient, CqlClient}
import org.joda.time.DateTime

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  private val timeout = Duration("5 seconds")

  def main(args: Array[String]) {

    val system = ActorSystem("helen-system")

    val client = new ActorBackedCqlClient("localhost", 9042)(system)

    //    sendAndPrint(client, Requests.Startup)
    println("1 ***********")
    setupKeyspaceTable(client)

    println("2 ***********")
    useOptions(client)

    println("3 ***********")
    insertSimple(client)
    println("4 ***********")
    insertWithPrepareExecute(client)
    println("5 ***********")
    insertWithValues(client)
    println("6 ***********")
    batchInsert(client)

    println("7 ***********")
    selectSimple(client)
    println("8 ***********")
    selectWithPrepareExecute(client)
    println("9 ***********")
    selectWithValues(client)
    println("10 ***********")
    selectWithValues2(client)

    println("11 ***********")
    insertSelectTuple(client)
    println("12 ***********")

    client.close()
    system.shutdown()
  }

  private def sendAndPrint(client: CqlClient, requests: Request*): Response = {
    var last: Response = null
    for (req <- requests) {
      last = Await.result(client.send(req), timeout)
      println(last)
    }
    last
  }

  private def useOptions(client: CqlClient) {
    sendAndPrint(client, Requests.Options)
  }

  private def setupKeyspaceTable(client: CqlClient) {
    sendAndPrint(client,
      Requests.Query("DROP KEYSPACE demodb"),
      Requests.Query("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"),
      Requests.Query("CREATE TABLE demodb.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob)"),
      Requests.Query(
        """CREATE TABLE demodb.songs2 (
          |id uuid PRIMARY KEY,
          |title text,
          |album varchar,
          |artist ascii,
          |good boolean,
          |rating int,
          |published timestamp,
          |address inet,
          |members list<int>,
          |justMap map<text, boolean>,
          |tags set<text>,
          |data blob)""".stripMargin),
      Requests.Query("CREATE TABLE demodb.tuple_test (the_key int PRIMARY KEY, the_tuple frozen<tuple<int, text, float>>)")
    )
  }

  private def insertSimple(client: CqlClient) {
    val query = Requests.Query("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES " +
      "(444446f7-2e54-4715-9f00-91dcbea6cf50, 'super title', 'super album', 'super artist', {'hello', 'world'})")
    sendAndPrint(client, query)
  }

  private def insertWithPrepareExecute(client: CqlClient) {

    val prepared = sendAndPrint(client,
      Requests.Prepare("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)")
    ).asInstanceOf[Prepared]

    val boundValues = List(
      UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50").asUUID,
      "La Petite Tonkinoise".asText,
      "Bye Bye Blackbird".asText,
      "Josephine Baker".asText,
      asSet(Set("jazz", "2014"), asText)
    )

    sendAndPrint(client,
      Requests.Execute(prepared.id, QueryParameters(values = boundValues))
    )
  }

  private def insertWithValues(client: CqlClient) {
    val vals = List(
      UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50").asUUID,
      "myTitle".asText,
      "Bye Bye Blackbird".asVarchar,
      "Josephine Baker".asAscii,
      true.asBoolean,
      42.asInt,
      DateTime.now.asTimestamp,
      InetAddress.getByName("192.168.1.1").asInet,
      asList(List(1, 2), asInt),
      asMap(Map("true" -> true, "false" -> false), asText, asBoolean),
      asSet(Set("jazz", "2013"), asText),
      ByteString.fromString("some random text to save")
    )

    sendAndPrint(client,
      Requests.Query("INSERT INTO demodb.songs2 (id, title, album, artist, good, rating, published, address, members, justMap, tags, data) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        QueryParameters(values = vals))
    )
  }

  private def batchInsert(client: CqlClient) {
    val queries = List(
      UnpreparedBatchQuery("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (999716f7-2e54-4715-9f00-91dcbea6cf50, 'hello', 'world', 'author', {'rock', '2014'})", Nil),
      UnpreparedBatchQuery("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (888716f7-2e54-4715-9f00-91dcbea6cf50, 'hello1', 'world1', 'author1', {'rock', '2014'})", Nil)
    )
    sendAndPrint(client, Requests.Batch(queries))
  }

  private def selectSimple(client: CqlClient) {
    sendAndPrint(client, Requests.Query("SELECT * FROM demodb.songs"))
  }

  private def selectWithPrepareExecute(client: CqlClient) {
    val prepared = sendAndPrint(client, Requests.Prepare("SELECT * FROM demodb.songs WHERE id = ?")
    ).asInstanceOf[Prepared]

    val boundValues = List(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50").asUUID)

    sendAndPrint(client, Requests.Execute(prepared.id, QueryParameters(values = boundValues)))
  }

  private def selectWithValues(client: CqlClient) {
    val boundValues = List(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50").asUUID)

    sendAndPrint(client,
      Requests.Query("SELECT * FROM demodb.songs WHERE id = ?", QueryParameters(values = boundValues))
    )
  }

  private def selectWithValues2(client: CqlClient) {
    val rows = sendAndPrint(client, Requests.Query("SELECT * FROM demodb.songs2")).asInstanceOf[Rows]

    val firstRow = rows.content(0)
    val id = firstRow(0).get.fromUUID
    val address = firstRow(1).get.fromInet
    val album = firstRow(2).get.fromText
    val artist = firstRow(3).get.fromAscii
    val data = firstRow(4).get.utf8String
    val good = firstRow(5).get.fromBoolean
    val justMap = fromMap(firstRow(6).get, fromText, fromBoolean)
    val members = fromList(firstRow(7).get, fromInt)
    val published = firstRow(8).get.fromTimestamp
    val rating = firstRow(9).get.fromInt
    val tags = fromSet(firstRow(10).get, fromText)
    val title = firstRow(11).get.fromText

    println(s"RESULT : $id $address $album $artist $data $good $justMap $members $published $rating $tags $title")
  }

  private def insertSelectTuple(client: CqlClient) {
    val query = Requests.Query("INSERT INTO demodb.tuple_test (the_key, the_tuple) VALUES (2, (1, 'abc', 3.14))")
    sendAndPrint(client, query)

  }
}
