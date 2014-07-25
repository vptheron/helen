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

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.helen.{Response => HResponse, Client}

import scala.concurrent.{ExecutionContext, Future}

private[native] class NativeClient(actor: ActorRef, eventHandler: ActorRef) extends Client {

  import ConnectionActor._

  implicit val timeout = Timeout(5, TimeUnit.SECONDS)

  override def query(q: String): Future[HResponse] =
    (actor ? Query(q)).mapTo[HResponse]

  private[native] def connect(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- actor ? Initialize
     // _ <- eventHandler ? EventHandlerActor.Initialize
    } yield Unit

  override def close(): Future[Unit] =
    (actor ? Close).mapTo[Unit]
}
