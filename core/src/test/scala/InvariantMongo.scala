package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

import mongoutils.BSONObjectConversions._

class InvariantMongoSpec extends FlatSpec with Matchers with MongodbTemporary {

    val fixture = BlogFixtures(s"extropy-spec-${System.currentTimeMillis}")
    import fixture._

    behavior of "containers (with mongo)"

    it should "iterate" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack, userCatLady)
        users.asLocation.iterator(mongoBackendClient).size should be(3)
        posts.asLocation.iterator(mongoBackendClient).size should be(2)
        comments.asLocation.iterator(mongoBackendClient).size should be(1)
    }

    behavior of "location interaction with mongo"

    it should "expand correctly" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack, userCatLady)
        IdLocation(posts,"post1").resolve(mongoBackendClient) should be( Iterable(IdLocation(posts,"post1")) )
        DocumentLocation(users,userLiz).resolve(mongoBackendClient) should be( Iterable(DocumentLocation(users,userLiz)) )
        QueryLocation(TopLevelContainer(s"$dbName.users"), IdLocation(posts,"post1"), "authorId").resolve(mongoBackendClient) should be(
            Traversable(IdLocation(users,"liz"))
        )
    }

    behavior of "fix one..."

    it should "fixOne searchableTitle" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1)
        searchableTitleRule.fixOne(mongoBackendClient, IdLocation(posts,"post1"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("searchableTitle" -> "title for post 1"))
        )
    }

    it should "fixOne authorNameInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1)
        mongoBackendClient(dbName)("users").insert(userLiz)
        authorNameInPostRule.fixOne(mongoBackendClient, IdLocation(posts,"post1"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("authorName" -> "Elizabeth Lemon"))
        )
    }

    it should "fixOne postCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1,post2)
        mongoBackendClient(dbName)("users").insert(userLiz)
        postCountInUserRule.fixOne(mongoBackendClient, IdLocation(users,"liz"))
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "liz")) should be(
            Some(userLiz ++ ("postCount" -> 2))
        )
    }

    it should "fixOne commentCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2)
        mongoBackendClient(dbName)("users").insert(userJack)
        commentCountInUserRule.fixOne(mongoBackendClient, IdLocation(users,"jack"))
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "jack")) should be(
            Some(userJack ++ ("commentCount" -> 1))
        )
    }

    it should "fixOne authorNameInCommentRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2)
        mongoBackendClient(dbName)("users").insert(userJack)
        authorNameInCommentRule.fixOne(mongoBackendClient, NestedIdLocation(comments,"post2",IdSubDocumentLocationFilter("comment1")))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[List[DBObject]]("comments").get should be(
            List(comment1 ++ ("authorName" -> """John Francis "Jack" Donaghy"""))
        )
    }

    it should "fixOne commentCountInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2)
        commentCountInPostRule.fixOne(mongoBackendClient, IdLocation(posts, "post2"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[Int]("commentCount").get should be(1)
    }

    behavior of "fix all..."

    it should "fix all searchableTitle" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        searchableTitleRule.fixAll(mongoBackendClient)

        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")).get should be(
            post1 ++ ("searchableTitle" -> "title for post 1")
        )
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.get("searchableTitle") should be( "title for post 2" )
    }

    it should "fix all authorNameInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        authorNameInPostRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")).get.get("authorName") should be("Elizabeth Lemon")
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.get("authorName") should be("Elizabeth Lemon")
    }

    it should "fix all postCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1,post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        postCountInUserRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "liz")) should be(
            Some(userLiz ++ ("postCount" -> 2))
        )
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "jack")) should be(
            Some(userJack ++ ("postCount" -> 0))
        )
    }

    it should "fix all commentCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        commentCountInUserRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "liz")) should be(
            Some(userLiz ++ ("commentCount" -> 0))
        )
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "jack")) should be(
            Some(userJack ++ ("commentCount" -> 1))
        )
    }

    it should "fix all authorNameInCommentRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        authorNameInCommentRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[List[DBObject]]("comments").get should be(
            List(comment1 ++ ("authorName" -> """John Francis "Jack" Donaghy"""))
        )
    }

    it should "fix all commentCountInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        commentCountInPostRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[Int]("commentCount").get should be(1)
    }

    behavior of "check all..."

    it should "check all searchableTitle" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        var errors = searchableTitleRule.checkAll(mongoBackendClient)
        errors should have size(2)

        searchableTitleRule.fixAll(mongoBackendClient)
        errors = searchableTitleRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all authorNameInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        var errors = authorNameInPostRule.checkAll(mongoBackendClient)
        errors should have size(2)

        authorNameInPostRule.fixAll(mongoBackendClient)
        errors = authorNameInPostRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all postCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        var errors = postCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(2)

        postCountInUserRule.fixAll(mongoBackendClient)
        errors = postCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all commentCountInUserRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        var errors = commentCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(2)

        commentCountInUserRule.fixAll(mongoBackendClient)
        errors = commentCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all authorNameInCommentRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        var errors = authorNameInCommentRule.checkAll(mongoBackendClient)
        errors should have size(1)

        authorNameInCommentRule.fixAll(mongoBackendClient)
        errors = authorNameInCommentRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all commentCountInPostRule" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        var errors = commentCountInPostRule.checkAll(mongoBackendClient)
        errors should have size(2)

        commentCountInPostRule.fixAll(mongoBackendClient)
        errors = commentCountInPostRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }
}
