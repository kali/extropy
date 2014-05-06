package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import com.mongodb.casbah.Imports._

class MongodbTemporarySpec extends FlatSpec with MongodbTemporary with ShouldMatchers {
    behavior of "A temporary mongo"

    it should "be running" in {
        mongoBackendClient("test")("col").drop()
        mongoBackendClient("test")("col").count() should be(0)
        mongoBackendClient("test")("col").save(MongoDBObject("a" -> 2))
        mongoBackendClient("test")("col").count() should be(1)
    }
}
