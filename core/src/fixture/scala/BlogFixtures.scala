package org.zoy.kali.extropy

import com.mongodb.casbah.Imports._

/*
users: ( _id, name, postCount*, commentCount* )
posts: ( _id, authorId, authorName*, title, searchableTitle*, comments[ { authorId, authorName* } ] )
*/

case class BlogFixtures(dbName:String) {

    // some rules
    val searchableTitleRule = Rule(     CollectionContainer(s"$dbName.posts"), CollectionContainer(s"$dbName.posts"),
                                        SameDocumentTie(), StringNormalizationReaction("title", "searchableTitle"))

    val authorNameInPostRule = Rule(    CollectionContainer(s"$dbName.posts"), CollectionContainer(s"$dbName.users"),
                                        FollowKeyTie("authorId"), CopyFieldsReaction(List(CopyField("name", "authorName"))))

    val postCountInUserRule = Rule(     CollectionContainer(s"$dbName.users"), CollectionContainer(s"$dbName.posts"),
                                        ReverseKeyTie("authorId"), CountReaction("postCount"))

    val commentCountInUserRule = Rule(  CollectionContainer(s"$dbName.users"), SubCollectionContainer(s"$dbName.posts", "comments"),
                                        ReverseKeyTie("authorId"), CountReaction("commentCount"))

    val authorNameInComment = Rule(     SubCollectionContainer(s"$dbName.posts","comments"), CollectionContainer(s"$dbName.users"),
                                        FollowKeyTie("authorId"), CopyFieldsReaction(List(CopyField("name", "authorName"))))

    val allRules = Array(searchableTitleRule, authorNameInPostRule, postCountInUserRule/*, commentCountInUserRule, authorNameInComment */) // FIXME

    // some monitored fields
    val monitorUsersId = MonitoredField(CollectionContainer(s"$dbName.users"), "_id") // id can not change, but this allow to detect insertion of users
    val monitorUsersName = MonitoredField(CollectionContainer(s"$dbName.users"), "name")
    val monitorPostsTitle = MonitoredField(CollectionContainer(s"$dbName.posts"), "title")
    val monitorPostsAuthorId = MonitoredField(CollectionContainer(s"$dbName.posts"), "authorId")
    val monitorPostsCommentsAuthorId = MonitoredField(SubCollectionContainer(s"$dbName.posts", "comments"), "authorId")

    // some data
    val userLiz = MongoDBObject("_id" -> "liz", "name" -> "Elizabeth Lemon")
    val userJack = MongoDBObject("_id" -> "jack", "name" -> "John Francis \"Jack\" Donaghy")
    val userCatLady = MongoDBObject("_id" -> "catLady")

    val post1 = MongoDBObject("_id" -> "post1", "title" -> "Title for Post 1", "authorId" -> "liz")
    val post2 = MongoDBObject("_id" -> "post2", "title" -> "Title for Post 2", "authorId" -> "liz",
                    "comments" -> MongoDBList(MongoDBObject("authorId" -> "jack")))

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

    // some full body updates
    val fbuUserLiz = FullBodyUpdateChange(s"$dbName.users", MongoDBObject("_id" -> "liz"), userLiz)
    val fbuPost1 = FullBodyUpdateChange(s"$dbName.posts", MongoDBObject("_id" -> "post1"), post1)

    // some delete
    val deleteUserLiz = DeleteChange(s"$dbName.users", MongoDBObject("_id" -> "liz"))
    val deletePost1 = DeleteChange(s"$dbName.posts", MongoDBObject("_id" -> "post1"))
}
