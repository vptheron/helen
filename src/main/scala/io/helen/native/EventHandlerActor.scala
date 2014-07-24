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
import akka.io.{Tcp, IO}
import io.helen.native.frames.{Responses, Requests}

class EventHandlerActor(host: String, port: Int) extends Actor {

  import EventHandlerActor.{Initialize, Close}

  implicit val sys = context.system

  def receive = {
    case Initialize =>
      println("Initialize event handler")
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
      println("Got: "+response)
      response match {
        case Responses.Ready(0x00) =>
          println("FIRST READY!")
          connection ! Tcp.Write(Requests.register(1.toByte).toData)

        case Responses.Ready(0x01) =>
          println("REGISTERED!")
          client ! Status.Success(Unit.box())
          context.become(waitForEvents(connection))

        case other =>
          println("Nothing matched: " + other)
          client ! Status.Failure(new Exception("Failed to connect: " + other))
      }
  }

  private def waitForEvents(connection: ActorRef): Receive = {
    case Tcp.Received(data) =>
      val event = Responses.fromData(data)
      println("received: "+event)

    case Close =>
      connection ! Tcp.Close
  }

}

object EventHandlerActor {

  def props(host: String, port: Int): Props = Props(new EventHandlerActor(host, port))

  case object Initialize

  case object Close

}
