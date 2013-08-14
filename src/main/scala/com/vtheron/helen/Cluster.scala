package com.vtheron.helen

import com.datastax.driver.core.{Cluster => JCluster, ProtocolOptions}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.vtheron.helen.Cluster.{NoCompression, Compression}

class Cluster(nodes: List[String],
              port: Int = Cluster.defaultPort,
              compression: Compression = NoCompression) {

  require(nodes != null && !nodes.isEmpty, "Cannot create a Cluster without a node address.")

  private val jCluster = JCluster.builder()
    .addContactPoints(nodes: _*)
    .withPort(port)
    //.withCredentials()
    .withCompression(compression.javaValue)
//    .withSSL(null)
    .build()

  def newSession(keySpace: Option[String] = None): Session =
    new Session(keySpace.fold(jCluster.connect())(ks => jCluster.connect(ks)))

  def shutdown(timeout: Duration): Boolean =
    jCluster.shutdown(timeout.toMillis, TimeUnit.MILLISECONDS)

  def metadata: Metadata = new Metadata(jCluster.getMetadata)

}

object Cluster {

  val defaultPort = ProtocolOptions.DEFAULT_PORT

  sealed trait Compression {
    private[helen] def javaValue: ProtocolOptions.Compression
  }

  case object NoCompression extends Compression {
    private[helen] val javaValue = ProtocolOptions.Compression.NONE
  }

  case object Snappy extends Compression {
    private[helen] val javaValue = ProtocolOptions.Compression.SNAPPY
  }

}
