package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class InvariantMongoSpec extends FlatSpec with ShouldMatchers with MongodbTemporary {

    import BlogFixtures._

    behavior of "data fixing"

    it should "fixOne searchableTitle" in {
        mongoBackendClient("blog").dropDatabase
        mongoBackendClient("blog")("posts").insert(post1)
        searchableTitleRule.fixOne(mongoBackendClient, IdLocation("post1"))
        mongoBackendClient("blog")("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("searchableTitle" -> "title for post 1"))
        )
    }
}
