package org.zoy.kali.extropy

import org.scalatest._

import com.mongodb.casbah.Imports._

class MongodbTemporarySpec extends FlatSpec with MongodbTemporary with Matchers {
    behavior of "A temporary mongo"

    it should "be running" in {
        mongoBackendClient("test")("col").drop()
        mongoBackendClient("test")("col").count() should be(0)
        mongoBackendClient("test")("col").save(MongoDBObject("a" -> 2))
        mongoBackendClient("test")("col").count() should be(1)
    }
}
