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

import com.datastax.driver.core.{Session => JSession, ResultSet}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.collection.convert.Wrappers.JListWrapper
import scala.concurrent.{Future, promise}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.vtheron.helen.{Row, Session}
import com.typesafe.scalalogging.slf4j.Logging

private[datastax] class DSSession(jSession: JSession) extends Session with Logging {

  def execute(query: String): Future[List[Row]] = {
    val asyncResult = promise[List[Row]]

    val resultSetFuture = jSession.executeAsync(query)

    Futures.addCallback(resultSetFuture, new FutureCallback[ResultSet] {
      def onFailure(t: Throwable) {
        asyncResult.failure(t)
      }

      def onSuccess(result: ResultSet) {
        asyncResult.success(
          new JListWrapper(result.all()).map(r => new DSRow(r)).toList)
      }
    })

    asyncResult.future
  }

  def close(timeout: Duration): Boolean = {
    logger.debug("Closing session...")
    jSession.shutdown(timeout.toMillis, TimeUnit.MILLISECONDS)
  }

}
