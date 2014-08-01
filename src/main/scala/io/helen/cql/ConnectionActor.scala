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

  import akka.io.Tcp._
  import io.helen.cql.ConnectionActor._

  implicit val sys = context.system

  def receive = {
    case Initialize =>
      IO(Tcp) ! Connect(new InetSocketAddress(host, port))
      context.become(waitForConnection(sender()))

    case other =>
      sender ! Status.Failure(
        new Exception("ConnectionActor not connected. `Initialize` should be the first message sent to this actor."))
  }

  private def waitForConnection(initializer: ActorRef): Receive = {
    case Connected(_, _) =>
      val connection = sender()
      connection ! Register(self)
      initializer ! Status.Success(Unit)
      context.become(ready(connection, allStreams, Map.empty))

    case CommandFailed(_: Connect) =>
      initializer ! Status.Failure(new Exception("Failed to connect to " + host))
      context stop self

    case other =>
      sender ! Status.Failure(new Exception("Waiting for connection."))
  }

  private def ready(connection: ActorRef, availableStreams: Set[Byte], processing: Map[Byte, ActorRef]): Receive = {
    case req: Request =>
      if (availableStreams.isEmpty) {
        sender ! Status.Failure(new Exception("Connection reached max pending requests."))
      } else {
        val stream = availableStreams.head
        connection ! Write(Frames.fromRequest(stream, req))
        context.become(ready(connection, availableStreams.tail, processing + (stream -> sender)))
      }

    case CommandFailed(Write(data, _)) =>
      val (stream, request) = Frames.fromBytes(data)
      processing.get(stream).foreach(_ ! Status.Failure(new Exception("Failed to send request: " + request)))
      context.become(ready(connection, availableStreams + stream, processing - stream))

    case Received(data) =>
      val (stream, response) = Frames.fromBytes(data)
      processing.get(stream).foreach(_ ! Status.Success(response))
      context.become(ready(connection, availableStreams + stream, processing - stream))

    case Terminate =>
      processing foreach {
        case (_, client) => client ! Status.Failure(new Exception("Connection closed by client."))
      }
      connection ! Close
      context.become(closing(sender()))

    case _: ConnectionClosed =>
      processing foreach {
        case (_, client) => client ! Status.Failure(new Exception("Connection unexpectedly closed."))
      }
      context stop self

    case other => sender ! Status.Failure(new Exception("Unrecognized message."))
  }

  private def closing(client: ActorRef): Receive = {
    case _: ConnectionClosed =>
      client ! Status.Success(Unit)
      context stop self

    case other =>
      sender ! Status.Failure(new Exception("This connection is closing and not accepting new requests."))
  }

}

private[cql] object ConnectionActor {

  def props(host: String, port: Int): Props = Props(new ConnectionActor(host, port))

  case object Initialize

  case object Terminate

  private val allStreams: Set[Byte] = (0 until 128).map(_.toByte).toSet
}
