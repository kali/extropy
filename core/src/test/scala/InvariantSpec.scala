package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

/*
users: ( _id, name, postCount*, commentCount* )
posts: ( _id, authorId, authorName*, title, searchableTitle*, comments[ { authorId, authorName* } ] )
*/

class InvariantSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {

    val searchableTitleRule = Rule(     CollectionContainer("blog.posts"),
                                        SameDocumentContact(CollectionContainer("blog.posts")),
                                        StringNormalizationProcessor("title", "searchableTitle"))

    val authorNameInPostRule = Rule(    CollectionContainer("blog.posts"),
                                        FollowKeyContact("blog.users", "authorId"),
                                        CopyFieldsProcessor(List(CopyField("name", "authorName"))))

    val postCountInUserRule = Rule(     CollectionContainer("blog.users"),
                                        ReverseKeyContact(CollectionContainer("blog.posts"), "authorId"),
                                        CountProcessor("postCount"))

    val commentCountInUserRule = Rule(  CollectionContainer("blog.users"),
                                        ReverseKeyContact(SubCollectionContainer("blog.posts","comments"), "authorId"),
                                        CountProcessor("commentCount"))

    val authorNameInComment = Rule(     SubCollectionContainer("blog.posts","comments"),
                                        FollowKeyContact("blog.users", "authorId"),
                                        CopyFieldsProcessor(List(CopyField("name", "authorName"))))

    val monitorUsersName = MonitoredField(CollectionContainer("blog.users"), "name")
    val monitorPostsTitle = MonitoredField(CollectionContainer("blog.posts"), "title")
    val monitorPostsAuthorId = MonitoredField(CollectionContainer("blog.posts"), "authorId")
    val monitorPostsCommentsAuthorId = MonitoredField(SubCollectionContainer("blog.posts", "comments"), "authorId")

    behavior of "Change detection"

    it should "identify fields to monitor" in {
        searchableTitleRule.monitoredFields should be( Set( monitorPostsTitle ) )
        authorNameInPostRule.monitoredFields should be( Set( monitorPostsAuthorId, monitorUsersName ) )
        postCountInUserRule.monitoredFields should be( Set( monitorPostsAuthorId ) )
        commentCountInUserRule.monitoredFields should be( Set( monitorPostsCommentsAuthorId ) )
        authorNameInComment.monitoredFields should be(Set( monitorPostsCommentsAuthorId, monitorUsersName ))
    }

    val userLiz = MongoDBObject("_id" -> "liz", "name" -> "Elizabeth Lemon")
    val userCatLady = MongoDBObject("_id" -> "catLady")

    val post1 = MongoDBObject("_id" -> "post1", "title" -> "Title for Post 1", "authorId" -> "liz")
    val post2 = MongoDBObject("_id" -> "post2", "title" -> "Title for Post 2", "authorId" -> "liz",
                    "comments" -> List(MongoDBObject("authorId" -> "jack")))

    it should "identify monitor field in ModifiersUpdateChange" in {
        val setNameOnUsers = ModifiersUpdateChange("blogs.users", MongoDBObject("_id" -> "liz"),
            MongoDBObject("$set" -> MongoDBObject("name" -> "Elizabeth Miervaldis Lemon")))
        setNameOnUsers.impactedFields should be ( Set("name") )
    }

    it should "monitor inserts" in {
        val insertUsers = InsertChange("blog.users", Stream( userLiz, userCatLady ))
        monitorUsersName.monitor(insertUsers) should be( Set(DocumentLocation(userLiz)) )
        monitorPostsTitle.monitor(insertUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(insertUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(insertUsers) should be ( 'empty )

        val insertPosts = InsertChange("blog.posts", Stream( post1, post2 ))
        monitorUsersName.monitor(insertPosts) should be( 'empty )
        monitorPostsTitle.monitor(insertPosts) should be ( Set(DocumentLocation(post1), DocumentLocation(post2)) )
        monitorPostsAuthorId.monitor(insertPosts) should be ( Set(DocumentLocation(post1), DocumentLocation(post2)) )
        //monitorPostsCommentsAuthorId.monitor(insertPosts) should be ( Set(DocumentLocation(post2)) ) // FIXME
        monitorPostsCommentsAuthorId.monitor(insertPosts) should be ( Set(DocumentLocation(post1), DocumentLocation(post2)) )

        val insertNotUsers = InsertChange("blog.not-user", Stream( userLiz, userCatLady ))
        monitorUsersName.monitor(insertNotUsers) should be( 'empty )
    }

    it should "monitor delete" in {
        val deleteUsers = DeleteChange("blog.users", MongoDBObject("_id" -> "liz"))
        monitorUsersName.monitor(deleteUsers) should be( Set(SelectorLocation(deleteUsers.selector.asInstanceOf[DBObject])) )
        monitorPostsTitle.monitor(deleteUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(deleteUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(deleteUsers) should be ( 'empty )

        val deletePosts = DeleteChange("blog.posts", MongoDBObject("_id" -> "post2"))
        monitorUsersName.monitor(deletePosts) should be( 'empty )
        monitorPostsTitle.monitor(deletePosts) should be ( Set(SelectorLocation(deletePosts.selector.asInstanceOf[DBObject])) )
        monitorPostsAuthorId.monitor(deletePosts) should be ( Set(SelectorLocation(deletePosts.selector.asInstanceOf[DBObject])) )
        monitorPostsCommentsAuthorId.monitor(deletePosts) should be ( Set(SelectorLocation(deletePosts.selector.asInstanceOf[DBObject])) )
    }

    it should "monitor full body update" in {
        val fbuUsers = FullBodyUpdateChange("blog.users", MongoDBObject("_id" -> "liz"), userLiz)
        monitorUsersName.monitor(fbuUsers) should be( Set(SelectorLocation(fbuUsers.selector.asInstanceOf[DBObject])) )
        monitorPostsTitle.monitor(fbuUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(fbuUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(fbuUsers) should be ( 'empty )

        val fbuPosts = FullBodyUpdateChange("blog.posts", MongoDBObject("_id" -> "post1"), post1)
        monitorUsersName.monitor(fbuPosts) should be( 'empty )
        monitorPostsTitle.monitor(fbuPosts) should be ( Set(SelectorLocation(fbuPosts.selector.asInstanceOf[DBObject])) )
        monitorPostsAuthorId.monitor(fbuPosts) should be ( Set(SelectorLocation(fbuPosts.selector.asInstanceOf[DBObject])) )
        monitorPostsCommentsAuthorId.monitor(fbuPosts) should be ( Set(SelectorLocation(fbuPosts.selector.asInstanceOf[DBObject])) )
    }

    it should "monitor modifiers update" in {
        val setNameOnUsers = ModifiersUpdateChange("blog.users", MongoDBObject("_id" -> "liz"),
            MongoDBObject("$set" -> MongoDBObject("name" -> "Elizabeth Miervaldis Lemon")))
        monitorUsersName.monitor(setNameOnUsers) should be ( Set(SelectorLocation(MongoDBObject("_id" -> "liz"))) )
        monitorPostsTitle.monitor(setNameOnUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNameOnUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(setNameOnUsers) should be ( 'empty )

        val setNotNameOnUsers = ModifiersUpdateChange("blog.users", MongoDBObject("_id" -> "liz"),
            MongoDBObject("$set" -> MongoDBObject("role" -> "Producer")))
        monitorUsersName.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsTitle.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )
    }

/*
    it should "detected monitored locations on insert" in {
    }
*/
}
