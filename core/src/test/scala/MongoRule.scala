package org.zoy.kali.extropy

import org.scalatest._

import com.mongodb.casbah.Imports._
import org.bson.BSONObject

class MongoRuleSpec extends FlatSpec with Matchers {

    val fixture = BlogFixtures("blog")
    def m[A <: String, B] (elems: (A, B)*) : DBObject = MongoDBObject(elems:_*)
    import fixture._

    behavior of "Rule mongo serializer"

    it should "serialize searchableTitleRule" in {
        searchableTitleRule.toMongo should be( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.posts", "same" -> "blog.posts"),
            "searchableTitle" -> MongoDBObject(
                "mvel" -> "title.toLowerCase()",
                "using" -> List("title")
            )
        ))
    }

    it should "serialize authorNameInPostRule" in {
        authorNameInPostRule.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.posts", "follow" -> "authorId", "to" -> "blog.users" ),
            "authorName" -> "name"
        ))
    }

    it should "serialize postCountInUserRule" in {
        postCountInUserRule.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.users", "search" -> "blog.posts", "by" -> "authorId"),
            "postCount" -> MongoDBObject("count" -> true)
        ))
    }

    it should "serialize commentCountInUserRule" in {
        commentCountInUserRule.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.users", "search" -> "blog.posts.comments", "by" -> "authorId"),
            "commentCount" -> MongoDBObject( "count" -> true )
        ))
    }

    it should "serialize authorNameInCommentRule" in {
        authorNameInCommentRule.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.posts.comments", "follow" -> "authorId", "to" -> "blog.users"),
            "authorName" -> "name"
        ))
    }

    it should "serialize commentCountInPostRule" in {
        commentCountInPostRule.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.posts", "unwind" -> "comments"),
            "commentCount" -> MongoDBObject("count" -> true)
        ))
    }

    behavior of "Rule mongo de-serializer"

    it should "de serialize searchableTitleRule" in {
        Rule.fromMongo(searchableTitleRule.toMongo) should be(searchableTitleRule)
    }

    it should "de serialize authorNameInPostRule" in {
        Rule.fromMongo(authorNameInPostRule.toMongo) should be(authorNameInPostRule)
    }

    it should "de serialize authorNameInCommentRule" in {
        Rule.fromMongo(authorNameInCommentRule.toMongo) should be(authorNameInCommentRule)
    }

    it should "de serialize postCountInUserRule" in {
        Rule.fromMongo(postCountInUserRule.toMongo) should be(postCountInUserRule)
    }

    it should "de serialize commentCountInUserRule" in {
        Rule.fromMongo(commentCountInUserRule.toMongo) should be(commentCountInUserRule)
    }

    it should "de serialize commentCountInPostRule" in {
        Rule.fromMongo(commentCountInPostRule.toMongo) should be(commentCountInPostRule)
    }
}
