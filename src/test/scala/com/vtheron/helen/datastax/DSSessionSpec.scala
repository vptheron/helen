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

import org.specs2.mutable.Specification
import org.specs2.mock._
import com.datastax.driver.core.{Session => JSession}
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

class DSSessionSpec extends Specification with Mockito {

  "A DSSession" should {

    "properly call the underlying shutdown method" in {
      val jSession = mock[JSession]
      jSession.shutdown(10000, TimeUnit.MILLISECONDS) returns true
      jSession.shutdown(15000, TimeUnit.MILLISECONDS) returns false

      val session = new DSSession(jSession)

      session.close(Duration(10, TimeUnit.SECONDS)) must beTrue
      session.close(Duration(15, TimeUnit.SECONDS)) must beFalse
    }

  }

}
