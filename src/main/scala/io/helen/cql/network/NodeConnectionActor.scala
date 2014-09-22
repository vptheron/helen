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
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import io.helen.cql.Requests.Request

private[cql] class NodeConnectionActor(node: InetSocketAddress, connectionCount: Int) extends Actor {

  var router = {
    val routees = Vector.fill(connectionCount)(newWatchedActor)
    Router(RoundRobinRoutingLogic(), routees)
  }

  override def receive: Receive = {
    case r: Request => router.route(r, sender())

    case Terminated(routee) =>
      println("A routee just terminated, starting a new instance.")
      router = router.removeRoutee(routee)
      router = router.addRoutee(newWatchedActor)

    case NodeConnectionActor.Close =>
      println("Received Close request, terminating all routees.")
      router.routees.foreach(_.send(SingleConnectionActor.CloseConnection, self))
      context.become(closing())
  }

  private def closing(): Receive = {
    case Terminated(r) =>
      router = router.removeRoutee(r)
      if(router.routees.isEmpty){
        context stop self
      }

    case other =>
      sender ! Status.Failure(new Exception("Connection is closing and no longer accepting requests."))
  }

  private def newWatchedActor: ActorRefRoutee = {
    val r = context.actorOf(SingleConnectionActor.props(node))
    context.watch(r)
    ActorRefRoutee(r)
  }
}

private[cql] object NodeConnectionActor {

  case object Close

  def props(host: String, port: Int, connections: Int): Props =
    props(new InetSocketAddress(host, port), connections)

  def props(node: InetSocketAddress, connectionCount: Int): Props =
    Props(new NodeConnectionActor(node, connectionCount))
}
