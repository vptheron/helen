package com.vtheron.helen.datastax

import com.datastax.driver.core.{Cluster => JCluster, ProtocolOptions}
import com.datastax.driver.core.exceptions.DriverException
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}
import com.vtheron.helen.{Session, Cluster}

class DSCluster private(nodes: List[String],
                        port: Int)
  extends Cluster {

  private val jCluster: JCluster = JCluster.builder()
    .addContactPoints(nodes: _*)
    .withPort(port)
    .withCompression(ProtocolOptions.Compression.NONE)
    .build()

  def openSession(keySpace: Option[String] = None): Session = {
    val jSession = keySpace.fold(jCluster.connect())(ks => jCluster.connect(ks))
    new DSSession(jSession)
  }

  def shutdown(timeout: Duration): Boolean =
    jCluster.shutdown(timeout.toMillis, TimeUnit.MILLISECONDS)

}

object DSCluster {

  val defaultPort = ProtocolOptions.DEFAULT_PORT

  def apply(nodes: List[String], port: Int = defaultPort): Try[DSCluster] =
    Try(new DSCluster(nodes, port)) recoverWith {
      case de: DriverException => Failure(new Exception(de.getMessage))
    }

}
