package com.vtheron.helen

import com.datastax.driver.core.{Session => JSession, ResultSet}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.collection.convert.Wrappers.JListWrapper
import scala.concurrent.{Future, promise}
import com.google.common.util.concurrent.{FutureCallback, Futures}

class Session private[helen](jSession: JSession) {

  def execute(query: String): Future[List[Row]] = {
    val p = promise[List[Row]]

    val resultSetFuture = jSession.executeAsync(query)

    Futures.addCallback(resultSetFuture, new FutureCallback[ResultSet] {
      def onFailure(t: Throwable) {
        p.failure(t)
      }

      def onSuccess(result: ResultSet) {
        p.success(new JListWrapper(result.all()).map(r => new Row(r)).toList)
      }
    })

    p.future
  }

  def close(timeout: Duration): Boolean =
    jSession.shutdown(timeout.toMillis, TimeUnit.MILLISECONDS)

}
