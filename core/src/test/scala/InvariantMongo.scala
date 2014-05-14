package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class InvariantMongoSpec extends FlatSpec with Matchers with MongodbTemporary {

    val fixture = BlogFixtures(s"extropy-spec-${System.currentTimeMillis}")
    import fixture._

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
/*
        QueryLocation(SubTopLevelContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post2"), "authorId").expand(mongoBackendClient) should be(
            Iterable(IdLocation(users,"jack"))
        )
        an[Exception] should be thrownBy {
            SnapshotLocation(QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation(posts,"post1"), "authorId") ).expand(mongoBackendClient)
        }
*/
    }

    it should "snaphost relevant information" in {
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack, userCatLady)
/*
        QueryLocation(TopLevelContainer(s"$dbName.posts"), IdLocation(posts,"post1"), "authorId").snapshot(mongoBackendClient) should be(
            Iterable(QueryLocation(TopLevelContainer(s"$dbName.posts"), IdLocation(posts,"post1"), "authorId"))
        )
*/
/*
        SnapshotLocation(QueryLocation(TopLevelContainer(s"$dbName.posts"), IdLocation(posts,"post1"), "authorId") ).
            snapshot(mongoBackendClient) should be( Iterable( IdLocation(users,"liz") ) )
*/
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
        pending
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2)
        mongoBackendClient(dbName)("users").insert(userJack)
        commentCountInUserRule.fixOne(mongoBackendClient, IdLocation(users,"jack"))
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "jack")) should be(
            Some(userJack ++ ("commentCount" -> 1))
        )
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

    it should "fixOne commentCountInUserRule" in {
pending
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

    it should "fixOne commentCountInUserRule" in {
pending
        mongoBackendClient(dbName).dropDatabase
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        var errors = commentCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(2)

        commentCountInUserRule.fixAll(mongoBackendClient)
        errors = commentCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }
}
