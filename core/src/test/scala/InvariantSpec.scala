package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class InvariantSpec extends FlatSpec with Matchers {

    val fixture = BlogFixtures(s"extropy-spec-${System.currentTimeMillis}")
    import fixture._

    behavior of "Value computation internals"

    it should "follow ties for document locations" in {
        searchableTitleRule.tie.resolve(searchableTitleRule, DocumentLocation(post1)) should be( DocumentLocation(post1) )
        authorNameInPostRule.tie.resolve(authorNameInPostRule, DocumentLocation(post1)) should be( IdLocation("liz") )
        postCountInUserRule.tie.resolve(postCountInUserRule, DocumentLocation(userLiz)) should be( SelectorLocation(MongoDBObject("authorId" -> "liz")) )
    }

    it should "follow ties for id locations" in {
        searchableTitleRule.tie.resolve(searchableTitleRule, IdLocation("post1")) should be( IdLocation("post1") )
        authorNameInPostRule.tie.resolve(authorNameInPostRule, IdLocation("post1")) should be( QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId") )
        postCountInUserRule.tie.resolve(postCountInUserRule, IdLocation("liz")) should be( SelectorLocation(MongoDBObject("authorId" -> "liz")) )
    }

    behavior of "Impact detection"

    it should "identify fields to monitor" in {
        searchableTitleRule.monitoredFields should be( Set( monitorPostsTitle ) )
        authorNameInPostRule.monitoredFields should be( Set( monitorPostsAuthorId, monitorUsersName ) )
        postCountInUserRule.monitoredFields should be( Set( monitorPostsAuthorId, monitorUsersId ) )
        commentCountInUserRule.monitoredFields should be( Set( monitorPostsCommentsAuthorId, monitorUsersId ) )
        authorNameInComment.monitoredFields should be(Set( monitorPostsCommentsAuthorId, monitorUsersName ))
    }

    it should "identify monitor field in ModifiersUpdateChange" in {
        setNameOnUserLiz.impactedFields should be ( Set("name") )
    }

    it should "monitor inserts" in {
        monitorUsersName.monitor(insertUserLiz) should be( Set(DocumentLocation(userLiz)) )
        monitorPostsTitle.monitor(insertUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(insertUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(insertUserLiz) should be ( 'empty )

        monitorUsersName.monitor(insertPost1) should be( 'empty )
        monitorPostsTitle.monitor(insertPost1) should be ( Set(DocumentLocation(post1)) )
        monitorPostsAuthorId.monitor(insertPost1) should be ( Set(DocumentLocation(post1)) )
        monitorPostsCommentsAuthorId.monitor(insertPost1) should be ( Set(DocumentLocation(post1)) )

        monitorUsersName.monitor(insertNotUsers) should be( 'empty )
    }

    it should "monitor delete" in {
        monitorUsersName.monitor(deleteUserLiz) should be( Set(IdLocation("liz")) )
        monitorPostsTitle.monitor(deleteUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(deleteUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(deleteUserLiz) should be ( 'empty )

        monitorUsersName.monitor(deletePost1) should be( 'empty )
        monitorPostsTitle.monitor(deletePost1) should be ( Set(IdLocation("post1")) )
        monitorPostsAuthorId.monitor(deletePost1) should be ( Set(IdLocation("post1")) )
        monitorPostsCommentsAuthorId.monitor(deletePost1) should be ( Set(IdLocation("post1")) )
    }

    it should "monitor full body update" in {
        monitorUsersName.monitor(fbuUserLiz) should be( Set(IdLocation("liz")) )
        monitorPostsTitle.monitor(fbuUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(fbuUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(fbuUserLiz) should be ( 'empty )

        monitorUsersName.monitor(fbuPost1) should be( 'empty )
        monitorPostsTitle.monitor(fbuPost1) should be ( Set(IdLocation("post1")) )
        monitorPostsAuthorId.monitor(fbuPost1) should be ( Set(IdLocation("post1")) )
        monitorPostsCommentsAuthorId.monitor(fbuPost1) should be ( Set(IdLocation("post1")) )
    }

    it should "monitor modifiers update" in {
        monitorUsersName.monitor(setNameOnUserLiz) should be ( Set(IdLocation("liz")) )
        monitorPostsTitle.monitor(setNameOnUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNameOnUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(setNameOnUserLiz) should be ( 'empty )

        monitorUsersName.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsTitle.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )
    }

    behavior of "a searchableTitleRule-like rule"

    it should "identify dirty set for inserts" in {
        searchableTitleRule.dirtiedSet( insertUserLiz ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( insertPost1 ) should be ( Set(DocumentLocation(post1)) )
    }

    it should "identify dirty set for modifiers updates" in {
        searchableTitleRule.dirtiedSet( setTitleOnPost1 ) should be( Set(IdLocation("post1")) )
        searchableTitleRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        searchableTitleRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        searchableTitleRule.dirtiedSet( setAuthorIdOnPost1 ) should be( 'empty )
    }

    it should "identify dirty set for fbu updates" in {
        searchableTitleRule.dirtiedSet( fbuUserLiz ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( fbuPost1 ) should be ( Set(IdLocation("post1")) )
    }

    it should "identify dirty set for delete" in {
        searchableTitleRule.dirtiedSet( deleteUserLiz ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( deletePost1 ) should be ( Set(IdLocation("post1")) ) // TODO: empty would be better.
    }

    behavior of "a authorNameInPostRule rule"

    it should "identify dirty set for inserts" in {
        authorNameInPostRule.dirtiedSet( insertUserLiz ) should be ( Set(SelectorLocation(MongoDBObject("authorId" -> "liz"))) )
        authorNameInPostRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        authorNameInPostRule.dirtiedSet( insertPost1 ) should be ( Set(DocumentLocation(post1)) )
    }

    it should "identify dirty set for modifiers updates" in {
        authorNameInPostRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        authorNameInPostRule.dirtiedSet( setNameOnUserLiz ) should be( Set(SelectorLocation(MongoDBObject("authorId" -> "liz"))) )
        authorNameInPostRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        authorNameInPostRule.dirtiedSet( setAuthorIdOnPost1 ) should be( Set(IdLocation("post1")) )
    }

    it should "identify dirty set for fbu updates" in {
        authorNameInPostRule.dirtiedSet( fbuUserLiz ) should be ( Set(SelectorLocation(MongoDBObject("authorId" -> "liz"))) )
        authorNameInPostRule.dirtiedSet( fbuPost1 ) should be ( Set(IdLocation("post1")) )
    }

    it should "identify dirty set for delete" in {
        authorNameInPostRule.dirtiedSet( deleteUserLiz ) should be ( Set(SelectorLocation(MongoDBObject("authorId" -> "liz"))) ) // TODO: not sure about this one
        authorNameInPostRule.dirtiedSet( deletePost1 ) should be ( Set(IdLocation("post1")) ) // TODO: empty should probably be better
    }

    behavior of "a postCountInUserRule rule"

    it should "identify dirty set for inserts" in {
        postCountInUserRule.dirtiedSet( insertUserLiz ) should be ( Set(DocumentLocation(userLiz)) )
        postCountInUserRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        postCountInUserRule.dirtiedSet( insertPost1 ) should be ( Set(
            QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId") ) ) )
    }

    it should "identify dirty set for modifiers updates" in {
        postCountInUserRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setAuthorIdOnPost1 ) should be( Set(
            QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId") ) ) )
    }

    it should "identify dirty set for fbu updates" in {
        postCountInUserRule.dirtiedSet( fbuUserLiz ) should be ( Set(IdLocation("liz")) )
        postCountInUserRule.dirtiedSet( fbuPost1 ) should be ( Set(
            QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId") ) ) )
    }

    it should "identify dirty set for delete" in {
        postCountInUserRule.dirtiedSet( deleteUserLiz ) should be ( Set(IdLocation("liz")) ) // TODO: optimize me
        postCountInUserRule.dirtiedSet( deletePost1 ) should be ( Set(
            QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer(s"$dbName.posts"), IdLocation("post1"), "authorId") ) ) )
    }

    /*
    it should "back propagate locations detected by processor" in {
        val same = SameDocumentTie(CollectionContainer(s"$dbName.posts"))
        same.backPropagate(IdLocation( "post1"))) should be (IdLocation( "post1")))

        // when user information change, I need to flag dirty all posts with matching authorId
        val follow = FollowKeyTie(s"$dbName.users", "authorId")
        follow.backPropagate(IdLocation( "liz"))) should be (SelectorLocation(MongoDBObject("authorId" -> "liz")))

        // when authorId in a post change, I need to flag dirty users which id was the previous value, and the one with new value (post count)
        val reverseTop = ReverseKeyTie(CollectionContainer(s"$dbName.posts"), "authorId")
        reverseTop.backPropagate(IdLocation( "post1"))) should be (BeforeAndAfterIdLocation(CollectionContainer(s"$dbName.posts"), MongoDBObject("_id" -> "post1"), "authorId"))

        // when authorId in a comment change, I need to flag dirty users which id was the previous value, and the one with new value (comment count)
        val reverseSub = ReverseKeyTie(SubCollectionContainer(s"$dbName.posts","comments"), "authorId")
        reverseSub.backPropagate(SelectorLocation(MongoDBObject("comments._id" -> "comment1"))) should be (BeforeAndAfterIdLocation(SubCollectionContainer(s"$dbName.posts", "comments"), MongoDBObject("comments._id" -> "comment1"), "authorId"))

        // when user information change, I need to flag dirty all posts with matching authorId
/*
        FollowKeyTie(s"$dbName.users", "authorId"),
*/
    }
*/

/*
    it should "detected monitored locations on insert" in {
    }
*/

}
