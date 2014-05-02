package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class InvariantMongoSpec extends FlatSpec with ShouldMatchers with MongodbTemporary {

    val fixture = BlogFixtures(s"extropy-spec-${System.currentTimeMillis}")
    import fixture._

    behavior of "fix one..."

    it should "fixOne searchableTitle" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1)
        searchableTitleRule.fixOne(mongoBackendClient, IdLocation("post1"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("searchableTitle" -> "title for post 1"))
        )
    }

    it should "fixOne authorNameInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1)
        mongoBackendClient(dbName)("users").insert(userLiz)
        authorNameInPostRule.fixOne(mongoBackendClient, IdLocation("post1"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("authorName" -> "Elizabeth Lemon"))
        )
    }

    it should "fixOne postCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1,post2)
        mongoBackendClient(dbName)("users").insert(userLiz)
        postCountInUserRule.fixOne(mongoBackendClient, IdLocation("liz"))
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "liz")) should be(
            Some(userLiz ++ ("postCount" -> 2))
        )
    }

    behavior of "fix all..."

    it should "fix all searchableTitle" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        searchableTitleRule.activeSync(mongoBackendClient)

        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")).get should be(
            post1 ++ ("searchableTitle" -> "title for post 1")
        )
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.get("searchableTitle") should be( "title for post 2" )
    }

    it should "fix all authorNameInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        authorNameInPostRule.activeSync(mongoBackendClient)
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")).get.get("authorName") should be("Elizabeth Lemon")
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.get("authorName") should be("Elizabeth Lemon")
    }

    it should "fix all postCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1,post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        postCountInUserRule.activeSync(mongoBackendClient)
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "liz")) should be(
            Some(userLiz ++ ("postCount" -> 2))
        )
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "jack")) should be(
            Some(userJack ++ ("postCount" -> 0))
        )
    }

    behavior of "check all..."

    it should "check all searchableTitle" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        var errors = searchableTitleRule.checkAll(mongoBackendClient)
        errors should have size(2)

        searchableTitleRule.activeSync(mongoBackendClient)
        errors = searchableTitleRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all authorNameInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        var errors = authorNameInPostRule.checkAll(mongoBackendClient)
        errors should have size(2)

        authorNameInPostRule.activeSync(mongoBackendClient)
        errors = authorNameInPostRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all postCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        var errors = postCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(2)

        postCountInUserRule.activeSync(mongoBackendClient)
        errors = postCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }
}
