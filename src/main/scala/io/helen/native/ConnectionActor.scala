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

import java.net.InetSocketAddress

import akka.actor.{Props, Status, ActorRef, Actor}
import akka.io.{IO, Tcp}
import io.helen.native.frames.{Responses, Requests}
import io.helen.{Response => HResponse}

class ConnectionActor(host: String, port: Int) extends Actor {

  import ConnectionActor.{Initialize, Close, Query => AQuery}

  implicit val sys = context.system

  def receive = {
    case Initialize =>
      IO(Tcp) ! Tcp.Connect(new InetSocketAddress(host, port))
      context.become(waitForConnection(sender()))
  }

  private def waitForConnection(client: ActorRef): Receive = {
    case Tcp.Connected(_, _) =>
      val connection = sender()
      connection ! Tcp.Register(self)
      connection ! Tcp.Write(Requests.startup(0.toByte).toData)
      context.become(waitForReady(client, connection))

    case Tcp.CommandFailed(_: Tcp.Connect) =>
      client ! Status.Failure(new Exception("Failed to connect to "+host))
      context stop self
  }

  private def waitForReady(client: ActorRef, connection: ActorRef): Receive = {
    case Tcp.Received(data) =>
      val response = Responses.fromData(data)
      response match {
        case Responses.Ready(stream) =>
          println("READY!")
          client ! Status.Success(Unit.box())
          context.become(waitForQuery(connection))

        case other =>
          println("Nothing matched: " + other)
          client ! Status.Failure(new Exception("Failed to connect: " + other))
      }
  }

  private def waitForQuery(connection: ActorRef): Receive = {
    case AQuery(q) =>
      connection ! Tcp.Write(Requests.query(0.toByte, q).toData)
      context.become(waitingForResponse(connection, sender()))

    case Close =>
      connection ! Tcp.Close
  }

  private def waitingForResponse(connection: ActorRef, client: ActorRef): Receive = {

    case Tcp.Received(data) =>
      val dataIt = data.iterator
      val stream = dataIt.drop(2).getByte
      val opsCode = dataIt.getByte
      client ! HResponse(opsCode)
      context.become(waitForQuery(connection))
  }

}

object ConnectionActor {

  def props(host: String, port: Int): Props = Props(new ConnectionActor(host, port))

  case object Initialize

  case object Close

  case class Query(q: String)

}
