/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import com.mongodb.casbah.Imports._

/*
users: ( _id, name, postCount*, commentCount* )
posts: ( _id, authorId, authorName*, title, searchableTitle*, comments[ { authorId, authorName* } ] )
*/

case class BlogFixtures(dbName:String) {

    val users = TopLevelContainer(s"$dbName.users")
    val posts = TopLevelContainer(s"$dbName.posts")
    val comments = NestedContainer(posts, "comments")

    // some rules
    val searchableTitleRule = Rule(     posts, posts, SameDocumentTie(),
                                        Map("searchableTitle" -> JSReaction(
                                            "function(doc) doc.title.toLowerCase()", List("title"))))

    val authorNameInPostRule = Rule(    posts, users, FollowKeyTie("authorId"),
                                        Map("authorName" -> CopyFieldsReaction("name")))

    val postCountInUserRule = Rule(     users, posts, ReverseKeyTie("authorId"),
                                        Map("postCount" -> JSReaction("function(cursor) cursor.size()", List())))

    val commentCountInUserRule = Rule(  users, comments, ReverseKeyTie("authorId"),
                                        Map("commentCount" -> JSReaction("function(cursor) cursor.size()", List())))

    val authorNameInCommentRule = Rule( comments, users, FollowKeyTie("authorId"),
                                        Map("authorName" -> CopyFieldsReaction("name")))

    val commentCountInPostRule = Rule(  posts, comments, SubDocumentTie("comments"),
                                        Map("commentCount" -> JSReaction("function(cursor) cursor.size()", List())))

    val averageRatingInPostRule = Rule( posts, comments, SubDocumentTie("comments"),
                                        Map("averageRating" ->
        JSReaction("""function(cursor) {
                        var total=0;
                        for each (comment in cursor) total += comment.rating;
                        return total / cursor.size();
                    }""")))

    val allRules = Array( searchableTitleRule, authorNameInPostRule, postCountInUserRule,
        commentCountInUserRule, authorNameInCommentRule, commentCountInPostRule)

    // some monitored fields
    val monitorUsersId = MonitoredField(users, "_id") // id can not change, but this allow to detect insertion of users
    val monitorPostsId = MonitoredField(posts, "_id")
    val monitorCommentsId = MonitoredField(comments, "_id")
    val monitorUsersName = MonitoredField(users, "name")
    val monitorPostsTitle = MonitoredField(posts, "title")
    val monitorPostsAuthorId = MonitoredField(posts, "authorId")
    val monitorPostsCommentsAuthorId = MonitoredField(comments, "authorId")

    // some data
    val userLiz = MongoDBObject("_id" -> "liz", "name" -> "Elizabeth Lemon")
    val userJack = MongoDBObject("_id" -> "jack", "name" -> "John Francis \"Jack\" Donaghy")
    val userCatLady = MongoDBObject("_id" -> "catLady")

    val comment1 = MongoDBObject("_id" -> "comment1", "authorId" -> "jack", "rating" -> 4)
    val comment2 = MongoDBObject("_id" -> "comment2", "authorId" -> "liz", "rating" -> 2)
    val post1 = MongoDBObject("_id" -> "post1", "title" -> "Title for Post 1", "authorId" -> "liz")
    val post2 = MongoDBObject("_id" -> "post2", "title" -> "Title for Post 2", "authorId" -> "liz",
                    "comments" -> MongoDBList(comment1))

    import scala.util.Random
    val moreComments = {
        val random = new Random(121314)
        (3 until 100).map { i =>
            MongoDBObject("_id" -> s"comment${i}",
                "authorId" -> (List("liz","jack")(random.nextInt(2))),
                "rating" -> random.nextInt(5))
        }
    }

    // some inserts
    val insertUserLiz = InsertChange(s"$dbName.users", userLiz)
    val insertUserJack = InsertChange(s"$dbName.users", userJack)
    val insertUserCatLady = InsertChange(s"$dbName.users", userCatLady)
    val insertNotUsers = InsertChange(s"$dbName.not-user", userLiz)
    val insertPost1 = InsertChange(s"$dbName.posts", post1)
    val insertPost2 = InsertChange(s"$dbName.posts", post2)

    // some modifiers updates
    val setNameOnUserLiz = ModifiersUpdateChange(s"$dbName.users", MongoDBObject("_id" -> "liz"),
        MongoDBObject("$set" -> MongoDBObject("name" -> "Elizabeth Miervaldis Lemon")))
    val setTitleOnPost1 = ModifiersUpdateChange(s"$dbName.posts", MongoDBObject("_id" -> "post1"),
        MongoDBObject("$set" -> MongoDBObject("title" -> "Other title for post 1")))
    val setAuthorIdOnPost1 = ModifiersUpdateChange(s"$dbName.posts", MongoDBObject("_id" -> "post1"),
        MongoDBObject("$set" -> MongoDBObject("authorId" -> "jack")))
    val setNotNameOnUsers = ModifiersUpdateChange(s"$dbName.users", MongoDBObject("_id" -> "liz"),
        MongoDBObject("$set" -> MongoDBObject("role" -> "Producer")))
    val setAuthorIdOnComment1 = ModifiersUpdateChange(s"$dbName.posts", MongoDBObject("comments._id" -> "comment1"),
        MongoDBObject("$set" -> MongoDBObject("comments.$.authorId" -> "liz")))
    val setAuthorIdOnCommentsNestedSel = ModifiersUpdateChange(s"$dbName.posts", MongoDBObject("comments.authorId" -> "jack"),
        MongoDBObject("$set" -> MongoDBObject("comments.$.authorId" -> "liz")))
    val pushCommentInPost1 = ModifiersUpdateChange(s"$dbName.posts", MongoDBObject("_id" -> "post1"),
        MongoDBObject("$push" -> MongoDBObject("comments" -> comment2)))

    // some full body updates
    val fbuUserLiz = FullBodyUpdateChange(s"$dbName.users", MongoDBObject("_id" -> "liz"), userLiz)
    val fbuPost1 = FullBodyUpdateChange(s"$dbName.posts", MongoDBObject("_id" -> "post1"), post1)
    val fbuPost2 = FullBodyUpdateChange(s"$dbName.posts", MongoDBObject("_id" -> "post2"), post2)

    // some delete
    val deleteUserLiz = DeleteChange(s"$dbName.users", MongoDBObject("_id" -> "liz"))
    val deletePost1 = DeleteChange(s"$dbName.posts", MongoDBObject("_id" -> "post1"))
}
