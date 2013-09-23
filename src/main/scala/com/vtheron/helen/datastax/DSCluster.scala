/*
 *      Copyright (C) 2013 Vincent Theron
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
package com.vtheron.helen.datastax

import com.datastax.driver.core.{Cluster => JCluster, ProtocolOptions}
import com.datastax.driver.core.exceptions.DriverException
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.util.{Failure, Try}
import com.vtheron.helen.{Session, Cluster}
import com.typesafe.scalalogging.slf4j.Logging

class DSCluster private(nodes: List[String],
                        port: Int)
  extends Cluster with Logging {

  private val jCluster: JCluster = JCluster.builder()
    .addContactPoints(nodes: _*)
    .withPort(port)
    .withCompression(ProtocolOptions.Compression.NONE)
    .build()

  def openSession(keySpace: Option[String] = None): Session =
    new DSSession(
      keySpace.fold(jCluster.connect())(ks => jCluster.connect(ks)))

  def shutdown(timeout: Duration): Boolean = {
    logger.debug("Cluster " + jCluster.getMetadata.getClusterName + " shutting down...")
    jCluster.shutdown(timeout.toMillis, TimeUnit.MILLISECONDS)
  }

}

object DSCluster extends Logging {

  val DEFAULT_PORT = ProtocolOptions.DEFAULT_PORT

  def apply(nodes: List[String], port: Int = DEFAULT_PORT): Try[DSCluster] =
    Try(new DSCluster(nodes, port)) recoverWith {
      case de: DriverException => {
        logger.warn("DriverException thrown when creating Cluster instance.", de)
        Failure(new Exception(de.getMessage))
      }
    }

}
