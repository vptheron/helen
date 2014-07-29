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

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, Props, Status}
import akka.io.{IO, Tcp}
import io.helen.cql.Requests.Request

private[cql] class ConnectionActor(host: String, port: Int) extends Actor {

  import io.helen.cql.ConnectionActor._

  implicit val sys = context.system

  def receive = {
    case Initialize =>
      IO(Tcp) ! Tcp.Connect(new InetSocketAddress(host, port))
      context.become(waitForConnection(sender()))
  }

  private def waitForConnection(initializer: ActorRef): Receive = {
    case Tcp.Connected(_, _) =>
      val connection = sender()
      connection ! Tcp.Register(self)
      initializer ! Status.Success(Unit)
      context.become(ready(connection, allStreams, Map.empty))

    case Tcp.CommandFailed(_: Tcp.Connect) =>
      initializer ! Status.Failure(new Exception("Failed to connect to " + host))
      context stop self
  }

  private def ready(connection: ActorRef, availableStreams: Set[Byte], processing: Map[Byte, ActorRef]): Receive = {
    case req: Request =>
      val stream = availableStreams.head
      val frame = Frames.fromRequest(stream, req)
      println("Actor: Sending " + req)
      connection ! Tcp.Write(frame)

      context.become(ready(connection, availableStreams.tail, processing + (stream -> sender())))

    case Tcp.Received(data) =>
      val (stream, response) = Frames.fromBytes(data)
      println("Actor: Received "+response)
      processing.get(stream).foreach(_ ! Status.Success(response))

      context.become(ready(connection, availableStreams + stream, processing - stream))

    case Close =>
      processing.foreach(_._2 ! Status.Failure(new Exception("Connection has been closed by client.")))
      connection ! Tcp.Close
      sender() ! Status.Success(Unit)

      context stop self
  }

}

private[cql] object ConnectionActor {

  def props(host: String, port: Int): Props = Props(new ConnectionActor(host, port))

  case object Initialize

  case object Close

  private val allStreams: Set[Byte] = (0 until 128).map(_.toByte).toSet
}
