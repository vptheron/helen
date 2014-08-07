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
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import akka.util.Timeout
import io.helen.cql.Requests
import io.helen.cql.Requests.Request

import scala.concurrent.Await

private[cql] class NodeConnectionActor(node: InetSocketAddress, connectionCount: Int) extends Actor {

  var router = {
    val routees = Vector.fill(connectionCount)(newWatchedActor)
    Router(RoundRobinRoutingLogic(), routees)
  }

  override def receive: Receive = {
    case r: Request => router.route(r, sender())

    case Terminated(routee) =>
      router = router.removeRoutee(routee)
      router = router.addRoutee(newWatchedActor)

    case NodeConnectionActor.Close =>
      router.routees.foreach(_.send(SingleConnectionActor.Terminate, self))
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
    implicit val timeout = Timeout(5, TimeUnit.SECONDS)
    context.watch(r)
    Await.ready(r ? Requests.Startup, timeout.duration)   //FIXME clearly a problem here if connection fails
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
