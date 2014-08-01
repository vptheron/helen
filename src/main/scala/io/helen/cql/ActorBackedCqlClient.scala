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

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import io.helen.cql.Requests.Request
import io.helen.cql.Responses.Response

import scala.concurrent.{Await, Future}

class ActorBackedCqlClient(host: String, port: Int)
                          (implicit system: ActorSystem) extends CqlClient {

  import ConnectionActor._

  private implicit val timeout = Timeout(10, TimeUnit.SECONDS)
  private val actor = system.actorOf(ConnectionActor.props(host, port))
  Await.ready(actor ? Initialize, timeout.duration)

  override def send(request: Request): Future[Response] = (actor ? request).mapTo[Response]

  override def close(){
    Await.ready(actor ? Close, timeout.duration)
  }
}
