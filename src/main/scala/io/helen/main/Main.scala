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

import akka.actor.ActorSystem
import io.helen.cql.{ActorBackedCqlClient, Requests}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]) {
    val timeout = Duration("10 seconds")

    val system = ActorSystem("helen-system")

    val client = new ActorBackedCqlClient("localhost", 9042)(system)

    println(Await.result(client.send(Requests.Startup), timeout))

    println(Await.result(
      client.send(
        Requests.Query("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}")),
      timeout))

    println(Await.result(
      client.send(
        Requests.Query("CREATE TABLE demodb.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob)")),
      timeout))

    println(Await.result(
      client.send(
        Requests.Query("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50, 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', {'jazz', '2013'})")),
      timeout))

    println(Await.result(
      client.send(
        Requests.Query("SELECT id, title, artist FROM demodb.songs")),
      timeout))

    client.close()
  }

}
