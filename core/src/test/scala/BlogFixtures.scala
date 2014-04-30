package org.zoy.kali.extropy

import com.mongodb.casbah.Imports._

/*
users: ( _id, name, postCount*, commentCount* )
posts: ( _id, authorId, authorName*, title, searchableTitle*, comments[ { authorId, authorName* } ] )
*/

object BlogFixtures {

    // some rules
    val searchableTitleRule = Rule(     CollectionContainer("blog.posts"), CollectionContainer("blog.posts"),
                                        SameDocumentTie(), StringNormalizationReaction("title", "searchableTitle"))

    val authorNameInPostRule = Rule(    CollectionContainer("blog.posts"), CollectionContainer("blog.users"),
                                        FollowKeyTie("authorId"), CopyFieldsReaction(List(CopyField("name", "authorName"))))

    val postCountInUserRule = Rule(     CollectionContainer("blog.users"), CollectionContainer("blog.posts"),
                                        ReverseKeyTie("authorId"), CountReaction("postCount"))

    val commentCountInUserRule = Rule(  CollectionContainer("blog.users"), SubCollectionContainer("blog.posts", "comments"),
                                        ReverseKeyTie("authorId"), CountReaction("commentCount"))

    val authorNameInComment = Rule(     SubCollectionContainer("blog.posts","comments"), CollectionContainer("blog.users"),
                                        FollowKeyTie("authorId"), CopyFieldsReaction(List(CopyField("name", "authorName"))))

    // some monitored fields
    val monitorUsersId = MonitoredField(CollectionContainer("blog.users"), "_id") // id can not change, but this allow to detect insertion of users
    val monitorUsersName = MonitoredField(CollectionContainer("blog.users"), "name")
    val monitorPostsTitle = MonitoredField(CollectionContainer("blog.posts"), "title")
    val monitorPostsAuthorId = MonitoredField(CollectionContainer("blog.posts"), "authorId")
    val monitorPostsCommentsAuthorId = MonitoredField(SubCollectionContainer("blog.posts", "comments"), "authorId")

    // some data
    val userLiz = MongoDBObject("_id" -> "liz", "name" -> "Elizabeth Lemon")
    val userJack = MongoDBObject("_id" -> "jack", "name" -> "John Francis \"Jack\" Donaghy")
    val userCatLady = MongoDBObject("_id" -> "catLady")

    val post1 = MongoDBObject("_id" -> "post1", "title" -> "Title for Post 1", "authorId" -> "liz")
    val post2 = MongoDBObject("_id" -> "post2", "title" -> "Title for Post 2", "authorId" -> "liz",
                    "comments" -> MongoDBList(MongoDBObject("authorId" -> "jack")))

    // some inserts
    val insertUserLiz = InsertChange("blog.users", Stream( userLiz ))
    val insertUsers = InsertChange("blog.users", Stream( userLiz, userJack, userCatLady ))
    val insertNotUsers = InsertChange("blog.not-user", Stream( userLiz, userCatLady ))
    val insertPost1 = InsertChange("blog.posts", Stream( post1 ))
    val insertPosts = InsertChange("blog.posts", Stream( post1, post2 ))

    // some modifiers updates
    val setNameOnUserLiz = ModifiersUpdateChange("blog.users", MongoDBObject("_id" -> "liz"),
        MongoDBObject("$set" -> MongoDBObject("name" -> "Elizabeth Miervaldis Lemon")))
    val setTitleOnPost1 = ModifiersUpdateChange("blog.posts", MongoDBObject("_id" -> "post1"),
        MongoDBObject("$set" -> MongoDBObject("title" -> "Other title for post 1")))
    val setAuthorIdOnPost1 = ModifiersUpdateChange("blog.posts", MongoDBObject("_id" -> "post1"),
        MongoDBObject("$set" -> MongoDBObject("authorId" -> "jack")))
    val setNotNameOnUsers = ModifiersUpdateChange("blog.users", MongoDBObject("_id" -> "liz"),
        MongoDBObject("$set" -> MongoDBObject("role" -> "Producer")))

    // some full body updates
    val fbuUserLiz = FullBodyUpdateChange("blog.users", MongoDBObject("_id" -> "liz"), userLiz)
    val fbuPost1 = FullBodyUpdateChange("blog.posts", MongoDBObject("_id" -> "post1"), post1)

    // some delete
    val deleteUserLiz = DeleteChange("blog.users", MongoDBObject("_id" -> "liz"))
    val deletePost1 = DeleteChange("blog.posts", MongoDBObject("_id" -> "post1"))

}

