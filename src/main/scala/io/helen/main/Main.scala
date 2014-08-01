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
import io.helen.cql.Requests.{QueryParameters, Request, UnpreparedBatchQuery}
import io.helen.cql.Responses.{Prepared, Response, Rows}
import io.helen.cql.{ActorBackedCqlClient, CqlClient, Requests, Values}
import org.joda.time.DateTime

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  private val timeout = Duration("5 seconds")

  def main(args: Array[String]) {

    val system = ActorSystem("helen-system")

    val client = new ActorBackedCqlClient("localhost", 9042)(system)

    sendAndPrint(client, Requests.Startup)

    setupKeyspaceTable(client)

    useOptions(client)

    insertWithPrepareExecute(client)

    selectWithValues(client)
    selectWithPrepare(client)
    batchInsert(client)

    selectAll(client)

    crazyInsert(client)
    crazySelect(client)

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
          |data blob)""".stripMargin)
    )
  }

  private def insertWithPrepareExecute(client: CqlClient) {

    val prepared = sendAndPrint(client,
      Requests.Prepare("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)")
    ).asInstanceOf[Prepared]

    val boundValues = List(
      Values.uuidToBytes(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")),
      Values.textToBytes("La Petite Tonkinoise"),
      Values.textToBytes("Bye Bye Blackbird"),
      Values.textToBytes("Josephine Baker"),
      Values.setToBytes(Set("jazz", "2014"), Values.textToBytes)
    )

    sendAndPrint(client,
      Requests.Execute(prepared.id, QueryParameters(values = boundValues))
    )
  }

  private def selectWithValues(client: CqlClient) {
    val boundValues = List(Values.uuidToBytes(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")))

    sendAndPrint(client,
      Requests.Query("SELECT * FROM demodb.songs WHERE id = ?", QueryParameters(values = boundValues))
    )
  }

  private def batchInsert(client: CqlClient) {
    val queries = List(
      UnpreparedBatchQuery("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (999716f7-2e54-4715-9f00-91dcbea6cf50, 'hello', 'world', 'author', {'rock', '2014'})", Nil),
      UnpreparedBatchQuery("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (888716f7-2e54-4715-9f00-91dcbea6cf50, 'hello1', 'world1', 'author1', {'rock', '2014'})", Nil)
    )
    sendAndPrint(client, Requests.Batch(queries))
  }

  private def selectAll(client: CqlClient) {
    sendAndPrint(client, Requests.Query("SELECT * FROM demodb.songs"))
  }

  private def selectWithPrepare(client: CqlClient) {
    val prepared = sendAndPrint(client, Requests.Prepare("SELECT * FROM demodb.songs WHERE id = ?")
    ).asInstanceOf[Prepared]

    val boundValues = List(Values.uuidToBytes(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")))

    sendAndPrint(client, Requests.Execute(prepared.id, QueryParameters(values = boundValues)))
  }

  private def crazyInsert(client: CqlClient) {
    val vals = List(
      Values.uuidToBytes(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50")),
      Values.textToBytes("myTitle"),
      Values.varcharToBytes("Bye Bye Blackbird"),
      Values.asciiToBytes("Josephine Baker"),
      Values.booleanToBytes(true),
      Values.intToBytes(42),
      Values.dateTimeToBytes(DateTime.now),
      Values.inetToBytes(InetAddress.getByName("192.168.1.1")),
      Values.listToBytes(List(1, 2), Values.intToBytes),
      Values.mapToBytes(Map("true" -> true, "false" -> false), Values.textToBytes, Values.booleanToBytes),
      Values.setToBytes(Set("jazz", "2013"), Values.textToBytes),
      ByteString.fromString("some random text to save")
    )

    sendAndPrint(client,
      Requests.Query("INSERT INTO demodb.songs2 (id, title, album, artist, good, rating, published, address, members, justMap, tags, data) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        QueryParameters(values = vals))
    )
  }

  private def crazySelect(client: CqlClient) {
    val rows = sendAndPrint(client, Requests.Query("SELECT * FROM demodb.songs2")).asInstanceOf[Rows]

    val firstRow = rows.content(0)
    val id = Values.bytesToUUID(firstRow(0).get)
    val address = Values.bytesToInet(firstRow(1).get)
    val album = Values.bytesToText(firstRow(2).get)
    val artist = Values.bytesToAscii(firstRow(3).get)
    val data = firstRow(4).get.utf8String
    val good = Values.bytesToBoolean(firstRow(5).get)
    val justMap = Values.bytesToMap(firstRow(6).get, Values.bytesToText, Values.bytesToBoolean)
    val members = Values.bytesToList(firstRow(7).get, Values.bytesToInt)
    val published = Values.bytesToDateTime(firstRow(8).get)
    val rating = Values.bytesToInt(firstRow(9).get)
    val tags = Values.bytesToSet(firstRow(10).get, Values.bytesToText)
    val title = Values.bytesToText(firstRow(11).get)

    println(s"$id $address $album $artist $data $good $justMap $members $published $rating $tags $title")
  }

}
