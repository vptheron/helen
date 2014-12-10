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
import akka.actor.SupervisorStrategy._
import io.helen.cql.Frames
import io.helen.cql.Requests.Request
import io.helen.cql.Requests
import io.helen.cql.Responses.Ready

private[cql] class SingleConnectionActor(address: InetSocketAddress) extends Actor {

  import SingleConnectionActor._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3) {
      case _: ActorInitializationException => Stop
      case _: ActorKilledException => Stop
      case _: RawConnectionActor.CantConnect => Stop
      case _: RawConnectionActor.ConnectionClosed => Restart
      case _ => Escalate
    }

  private val connection = context.actorOf(RawConnectionActor.props(address, self))

  private def waitingForInitialized: Receive = {
    case RawConnectionActor.Initialized =>
      self ! Requests.Startup
      context.become(handlingRequests(Frames.allStreams, Map.empty))

    case other => sender ! Status.Failure(new Exception("Opening connection, not accepting request yet."))
  }

  private def handlingRequests(availableStreams: Set[Short], processing: Map[Short, ActorRef]): Receive = {
    case req: Request => availableStreams.headOption match {
      case None => sender ! Status.Failure(new Exception("Connection reached max pending requests."))

      case Some(stream) =>
        connection ! RawConnectionActor.WriteRequest(Frames.fromRequest(stream, req))
        context.become(handlingRequests(availableStreams.tail, processing + (stream -> sender)))
    }

    case RawConnectionActor.WriteFailed(data) =>
      val (stream, request) = Frames.fromBytes(data)
      processing.get(stream).foreach(_ ! Status.Failure(new Exception("Failed to send request: " + request)))
      context.become(handlingRequests(availableStreams + stream, processing - stream))

    case RawConnectionActor.WriteResponse(data) =>
      val (stream, response) = Frames.fromBytes(data)
      processing.get(stream).foreach(_ ! Status.Success(response))
      context.become(handlingRequests(availableStreams + stream, processing - stream))

    case CloseConnection =>
      processing foreach {
        case (_, client) => client ! Status.Failure(new Exception("Connection closed by client."))
      }
      connection ! RawConnectionActor.CloseRequest
      context stop self

    case m => println("Received unknown message: "+m)
  }

  override def receive = waitingForInitialized
}

private[cql] object SingleConnectionActor {

  def props(host: String, port: Int): Props = props(new InetSocketAddress(host, port))

  def props(address: InetSocketAddress): Props = Props(new SingleConnectionActor(address))

  case object CloseConnection

}