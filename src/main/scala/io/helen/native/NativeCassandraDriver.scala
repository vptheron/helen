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
package io.helen.native

import akka.actor.{Props, ActorSystem}
import io.helen.{Client, CassandraDriver}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class NativeCassandraDriver(system: ActorSystem) extends CassandraDriver {

  implicit private val ec = system.dispatcher

  override def connect(host: String, port: Int): Client = {
    val actor = system.actorOf(ConnectionActor.props(host, port))
    val eventHandler = system.actorOf(EventHandlerActor.props(host, port))
    val client = new NativeClient(actor, eventHandler)
    Await.result(client.connect(ec), Duration("5 seconds"))
    client
  }

}
