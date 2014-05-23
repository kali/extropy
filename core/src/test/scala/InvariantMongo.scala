package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

import mongoutils.BSONObjectConversions._

class InvariantMongoSpec extends FlatSpec with Matchers with MongodbTemporary {

    val fixture = BlogFixtures(s"extropy-spec-${System.currentTimeMillis}")
    import fixture._

    def truncateAll {
        mongoBackendClient(dbName)("users").remove(MongoDBObject.empty)
        mongoBackendClient(dbName)("posts").remove(MongoDBObject.empty)
    }

    behavior of "containers (with mongo)"

    it should "iterate" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack, userCatLady)
        users.asLocation.iterator(mongoBackendClient).size should be(3)
        posts.asLocation.iterator(mongoBackendClient).size should be(2)
        comments.asLocation.iterator(mongoBackendClient).size should be(1)
    }

    behavior of "MVEL reactions"

    it should "work again top level stuff" in {
        truncateAll
        mongoBackendClient(dbName)("users").insert(userLiz ++ ("rating" -> 12), userJack ++ ("rating" -> 42))
        import org.mvel2.MVEL
        import scala.collection.JavaConversions
        val compiled = MVEL.compileExpression("""
            total = 0;
            for(it: items) { total += it.get("rating") };
            total
        """)
        val context = new java.util.HashMap[String,AnyRef]
        context.put("items", JavaConversions.asJavaIterable( mongoBackendClient(dbName)("users").find(MongoDBObject.empty).map(_.toMap).toIterable ))
        MVEL.executeExpression(compiled, context) should be(54)
    }

    it should "work against inner stuff for one doc from memory" taggedAs(Tag("w")) in {
        import org.mvel2.MVEL
        import scala.collection.JavaConversions
/*
        truncateAll
*/
        val post2WithMoreComments = post2 ++ ("comments" -> (comment1 :: comment2 :: moreComments.toList))
//        mongoBackendClient(dbName)("posts").insert(post2WithMoreComments)
        val compiled = MVEL.compileExpression("""
            total = 0;
            for(it: items) { total += it.get("rating") };
            total
        """)
        val context = new java.util.HashMap[String,AnyRef]
        context.put("items", JavaConversions.asJavaIterable(
            post2WithMoreComments.as[MongoDBList]("comments").map( _.asInstanceOf[DBObject].toMap).toIterable
        ))
        MVEL.executeExpression(compiled, context) should be(54)
    }

    it should "work against inner stuff for a flatMap cursor/array" taggedAs(Tag("w")) in {
        import org.mvel2.MVEL
        import scala.collection.JavaConversions
        truncateAll
        val post2WithMoreComments = post2 ++ ("comments" -> (comment1 :: comment2 :: moreComments.toList))
        mongoBackendClient(dbName)("posts").insert(post2WithMoreComments)
        val compiled = MVEL.compileExpression("""
            total = 0;
            for(it: items) { total += it.get("rating") };
            total
        """)
        val context = new java.util.HashMap[String,AnyRef]
        context.put("items", JavaConversions.asJavaIterable(
            mongoBackendClient(dbName)("posts").find(MongoDBObject.empty).toIterable
                .flatMap( _.as[MongoDBList]("comments") )
                .map(_.asInstanceOf[DBObject].toMap).toIterable
        ))
        MVEL.executeExpression(compiled, context) should be(54)
    }

    behavior of "location interaction with mongo"

    it should "expand correctly" in {
        truncateAll
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
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1)
        searchableTitleRule.fixOne(mongoBackendClient, IdLocation(posts,"post1"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("searchableTitle" -> "title for post 1"))
        )
    }

    it should "fixOne authorNameInPostRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1)
        mongoBackendClient(dbName)("users").insert(userLiz)
        authorNameInPostRule.fixOne(mongoBackendClient, IdLocation(posts,"post1"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")) should be(
            Some(post1 ++ ("authorName" -> "Elizabeth Lemon"))
        )
    }

    it should "fixOne postCountInUserRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1,post2)
        mongoBackendClient(dbName)("users").insert(userLiz)
        postCountInUserRule.fixOne(mongoBackendClient, IdLocation(users,"liz"))
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "liz")) should be(
            Some(userLiz ++ ("postCount" -> 2))
        )
    }

    it should "fixOne commentCountInUserRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2)
        mongoBackendClient(dbName)("users").insert(userJack)
        commentCountInUserRule.fixOne(mongoBackendClient, IdLocation(users,"jack"))
        mongoBackendClient(dbName)("users").findOne(MongoDBObject("_id" -> "jack")) should be(
            Some(userJack ++ ("commentCount" -> 1))
        )
    }

    it should "fixOne authorNameInCommentRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2)
        mongoBackendClient(dbName)("users").insert(userJack)
        authorNameInCommentRule.fixOne(mongoBackendClient, NestedIdLocation(comments,"post2",IdSubDocumentLocationFilter("comment1")))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[List[DBObject]]("comments").get should be(
            List(comment1 ++ ("authorName" -> """John Francis "Jack" Donaghy"""))
        )
    }

    it should "fixOne commentCountInPostRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2)
        commentCountInPostRule.fixOne(mongoBackendClient, IdLocation(posts, "post2"))
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[Int]("commentCount").get should be(1)
    }

    it should "fixOne averageRatingInPostRule" in {
        truncateAll
        val post2WithMoreComments = post2 ++ ("comments" -> (comment1 :: comment2 :: moreComments.toList))
        mongoBackendClient(dbName)("posts").insert(post2WithMoreComments)
        averageRatingInPostRule.fixOne(mongoBackendClient, IdLocation(posts, "post2"))
        println(mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get)
    }

    behavior of "fix all..."

    it should "fix all searchableTitle" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        searchableTitleRule.fixAll(mongoBackendClient)

        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")).get should be(
            post1 ++ ("searchableTitle" -> "title for post 1")
        )
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.get("searchableTitle") should be( "title for post 2" )
    }

    it should "fix all authorNameInPostRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        authorNameInPostRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post1")).get.get("authorName") should be("Elizabeth Lemon")
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.get("authorName") should be("Elizabeth Lemon")
    }

    it should "fix all postCountInUserRule" in {
        truncateAll
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
        truncateAll
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
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        authorNameInCommentRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[List[DBObject]]("comments").get should be(
            List(comment1 ++ ("authorName" -> """John Francis "Jack" Donaghy"""))
        )
    }

    it should "fix all commentCountInPostRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        commentCountInPostRule.fixAll(mongoBackendClient)
        mongoBackendClient(dbName)("posts").findOne(MongoDBObject("_id" -> "post2")).get.getAs[Int]("commentCount").get should be(1)
    }

    behavior of "check all..."

    it should "check all searchableTitle" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        var errors = searchableTitleRule.checkAll(mongoBackendClient)
        errors should have size(2)

        searchableTitleRule.fixAll(mongoBackendClient)
        errors = searchableTitleRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all authorNameInPostRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        var errors = authorNameInPostRule.checkAll(mongoBackendClient)
        errors should have size(2)

        authorNameInPostRule.fixAll(mongoBackendClient)
        errors = authorNameInPostRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all postCountInUserRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post1, post2)
        mongoBackendClient(dbName)("users").insert(userLiz, userJack)
        var errors = postCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(2)

        postCountInUserRule.fixAll(mongoBackendClient)
        errors = postCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all commentCountInUserRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        var errors = commentCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(2)

        commentCountInUserRule.fixAll(mongoBackendClient)
        errors = commentCountInUserRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all authorNameInCommentRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        mongoBackendClient(dbName)("users").insert(userJack, userLiz)
        var errors = authorNameInCommentRule.checkAll(mongoBackendClient)
        errors should have size(1)

        authorNameInCommentRule.fixAll(mongoBackendClient)
        errors = authorNameInCommentRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }

    it should "check all commentCountInPostRule" in {
        truncateAll
        mongoBackendClient(dbName)("posts").insert(post2, post1)
        var errors = commentCountInPostRule.checkAll(mongoBackendClient)
        errors should have size(2)

        commentCountInPostRule.fixAll(mongoBackendClient)
        errors = commentCountInPostRule.checkAll(mongoBackendClient)
        errors should have size(0)
    }
}
