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
package io.helen.cql.network

import java.net.InetSocketAddress

import akka.actor._
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import io.helen.cql.Frames
import io.helen.cql.Requests.Request

private[cql] class SingleConnectionActor(address: InetSocketAddress) extends Actor with Stash {

  import SingleConnectionActor._

  implicit val sys = context.system

  IO(Tcp) ! Connect(address)

  override def receive = {
    case Connected(_, _) =>
      val connection = sender()
      connection ! Register(self)
      unstashAll()
      context.become(ready(connection, allStreams, Map.empty))

    case CommandFailed(_: Connect) =>
      //TODO report failure to supervisor
      context stop self

    case other => stash()
  }

  private def ready(connection: ActorRef, availableStreams: Set[Short], processing: Map[Short, ActorRef]): Receive = {
    case req: Request => availableStreams.headOption match {
      case None =>
        //TODO could use stash to  store the request and try later
        sender ! Status.Failure(new Exception("Connection reached max pending requests."))

      case Some(stream) =>
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
    case _: ConnectionClosed => context stop self

    case other =>
      sender ! Status.Failure(new Exception("This connection is closing and not accepting new requests."))
  }

}

private[cql] object SingleConnectionActor {

  def props(host: String, port: Int): Props = props(new InetSocketAddress(host, port))

  def props(address: InetSocketAddress): Props = Props(new SingleConnectionActor(address))

  case object Terminate

  private val allStreams: Set[Short] = (0 until 32768).map(_.toShort).toSet
}
