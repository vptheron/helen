package io.helen.cql

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import akka.pattern.ask
import akka.util.Timeout
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
