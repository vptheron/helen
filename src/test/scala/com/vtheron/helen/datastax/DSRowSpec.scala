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

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import com.datastax.driver.core.{Row => JRow}
import scala.collection.convert.WrapAsJava._

class DSRowSpec extends Specification with Mockito {

  "A DSRow" should {

    "be able to return a List of the appropriate class" in {
      val jRow = mock[JRow]
      jRow.getList(42, classOf[Int]) returns seqAsJavaList(List(1, 2, 3))

      val row = new DSRow(jRow)

      row.getIndexAsList[Int](42) must beEqualTo(List(1,2,3))
    }

  }

}
