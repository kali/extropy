/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import org.scalatest._

import com.mongodb.casbah.Imports._
import org.bson.BSONObject

case class MyReaction(foo:String, bar:Int) extends SalatReaction {
    def process(data:Traversable[BSONObject], multiple:Boolean) = Some("blah")
    def reactionFields = Set()
    def toLabel = "hey"
}

class MongoRuleSpec extends FlatSpec with Matchers {

    val fixture = BlogFixtures("blog")
    def m[A <: String, B] (elems: (A, B)*) : DBObject = MongoDBObject(elems:_*)
    import fixture._

    val ruleWithDot = Rule(
                    TopLevelContainer("db.dotted.collection.name"),
                    NestedContainer(TopLevelContainer("db.dotted.collection.name"), "field"),
                    SubDocumentTie("field"),
                    Map("foo" -> CopyFieldsReaction("bar")))

    val otherRuleWithDot = Rule(
                    NestedContainer(TopLevelContainer("db.dotted.collection.name"), "field"),
                    TopLevelContainer("db.dotted.collection.name"),
                    FollowKeyTie("field"),
                    Map("foo" -> CopyFieldsReaction("bar")))

    behavior of "Rule mongo serializer"

    it should "serialize searchableTitleRule" in {
        searchableTitleRule.toMongo should be( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.posts", "same" -> "blog.posts"),
            "searchableTitle" -> MongoDBObject(
                "js" -> "function(doc) doc.title.toLowerCase()",
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
            "postCount" -> MongoDBObject("js" -> "function(cursor) cursor.size()")
        ))
    }

    it should "serialize commentCountInUserRule" in {
        commentCountInUserRule.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "blog.users", "search" -> "blog.posts.comments", "by" -> "authorId"),
            "commentCount" -> MongoDBObject("js" -> "function(cursor) cursor.size()")
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
            "commentCount" -> MongoDBObject("js" -> "function(cursor) cursor.size()")
        ))
    }

    it should "serialize custom salat reaction" in {
        val rule = Rule( TopLevelContainer("foo.bar"), TopLevelContainer("foo.bar"), SameDocumentTie(),
                        Map("baz" -> MyReaction("qux", 42) ) )
        rule.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> "foo.bar", "same" -> "foo.bar"),
            "baz" -> MongoDBObject("_typeHint" -> "org.zoy.kali.extropy.MyReaction", "foo"->"qux", "bar"->42)
        ))
    }

    it should "serialize dotted name as an array" in {
        ruleWithDot.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> Seq("db", "dotted.collection.name"), "unwind" -> "field"),
            "foo" -> "bar"
        ))
        otherRuleWithDot.toMongo should be ( MongoDBObject(
            "rule" -> MongoDBObject("from" -> Seq("db", "dotted.collection.name", "field"),
                                    "follow" -> "field",
                                    "to" -> List("db", "dotted.collection.name")),
            "foo" -> "bar"
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

    it should "de serialize custom salat reaction" in {
        val rule = Rule( TopLevelContainer("foo.bar"), TopLevelContainer("foo.bar"), SameDocumentTie(),
                        Map("baz" -> MyReaction("qux", 42) ) )
        Rule.fromMongo(rule.toMongo) should be ( rule )
    }

    it should "deserialize dotted name as array" taggedAs(Tag("foo")) in {
        Rule.fromMongo(ruleWithDot.toMongo) should be (ruleWithDot)
        Rule.fromMongo(otherRuleWithDot.toMongo) should be (otherRuleWithDot)
    }
}
