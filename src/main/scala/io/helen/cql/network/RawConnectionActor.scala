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
import akka.util.ByteString

private[cql] class RawConnectionActor(address: InetSocketAddress,
                                         listener: ActorRef) extends Actor {

  import RawConnectionActor._

  IO(Tcp)(context.system) ! Connect(address)

  private def openingConnection: Receive = {
    case Connected(remote, local) =>
      val connection = sender()
      connection ! Register(self, useResumeWriting = false)

      listener ! Initialized

      context.become(handlingRequests(connection))

    case CommandFailed(c: Connect) => throw new CantConnect

    case other => listener ! Status.Failure(new Exception("Opening connection, not accepting request yet."))
  }

  private def handlingRequests(connection: ActorRef): Receive = {
    case WriteRequest(data) => connection ! Write(data)

    case CommandFailed(Write(data, _)) => listener ! WriteFailed(data)

    case Received(data) => listener ! WriteResponse(data)

    case CloseRequest =>
      connection ! Close
      context.become(closingConnection)

    case ErrorClosed | PeerClosed => throw new ConnectionClosed
  }

  private def closingConnection: Receive = {
    case Closed => context stop self

    case other => listener ! Status.Failure(new Exception("This connection is now closing."))
  }

  override def receive = openingConnection

}

private[cql] object RawConnectionActor {

  def props(host: String, port: Int, listener: ActorRef): Props = props(new InetSocketAddress(host, port), listener)

  def props(address: InetSocketAddress, listener: ActorRef): Props = Props(new RawConnectionActor(address, listener))

  case object Initialized

  case class WriteRequest(data: ByteString)

  case class WriteFailed(data: ByteString)

  case class WriteResponse(data: ByteString)

  case object CloseRequest

  class CantConnect extends Exception

  class ConnectionClosed extends Exception

}
