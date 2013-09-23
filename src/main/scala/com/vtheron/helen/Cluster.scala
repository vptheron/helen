package com.vtheron.helen

import scala.concurrent.duration.Duration

trait Cluster {

  def openSession(keySpace: Option[String] = None): Session

  def shutdown(timeout: Duration): Boolean
}