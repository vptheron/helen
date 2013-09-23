package com.vtheron.helen

import scala.concurrent.duration.Duration
import scala.concurrent.Future

trait Session {

  def execute(query: String): Future[List[Row]]

  def close(timeout: Duration): Boolean

}
