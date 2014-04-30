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

    it should "fixOne authorNameInPostRule" in {
        mongoBackendClient("blog").dropDatabase
        mongoBackendClient("blog")("posts").insert(post1)
        mongoBackendClient("blog")("users").insert(userLiz)
        authorNameInPostRule.fixOne(mongoBackendClient, IdLocation("post1"))
        mongoBackendClient("blog")("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("authorName" -> "Elizabeth Lemon"))
        )
    }

    it should "fixOne postCountInUserRule" in {
        mongoBackendClient("blog").dropDatabase
        mongoBackendClient("blog")("posts").insert(post1,post2)
        mongoBackendClient("blog")("users").insert(userLiz)
        postCountInUserRule.fixOne(mongoBackendClient, IdLocation("liz"))
        mongoBackendClient("blog")("users").findOne(MongoDBObject("_id" -> "liz")) should be(
            Some(userLiz ++ ("postCount" -> 2))
        )
    }
}
