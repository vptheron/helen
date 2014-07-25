/*
 *      Copyright (C) 2013 Vincent Theron
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
import io.helen.native.NativeCassandraDriver

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

   def main(args: Array[String]){
     val timeout = Duration("2 seconds")
     val system = ActorSystem("helen-system")
     val driver = new NativeCassandraDriver(system)
     val cluster = driver.connect("localhost", 9042)

//     Await.result(cluster.query("CREATE KEYSPACE demodb WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 1}"), timeout)

//     Await.result(cluster.query("CREATE TABLE demodb.songs (id uuid PRIMARY KEY, title text, album text, artist text, tags set<text>, data blob)"), timeout)
//     Await.result(cluster.query("INSERT INTO demodb.songs (id, title, album, artist, tags) VALUES (756716f7-2e54-4715-9f00-91dcbea6cf50, 'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', {'jazz', '2013'})"), timeout)
     Await.result(cluster.query("SELECT * FROM demodb.songs"), timeout)
   }

 }
