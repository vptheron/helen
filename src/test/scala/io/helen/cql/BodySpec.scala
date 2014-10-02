package io.helen.cql

import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks

class BodySpec extends PropSpec with Matchers with PropertyChecks {

  import Body._

  property("Strings can be serialized and deserialized") {
    forAll { s: String =>
      val bytes = string(s)
      val res = readString(bytes.iterator)
      res shouldBe s
    }
  }

}
