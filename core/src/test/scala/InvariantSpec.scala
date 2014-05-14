package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class InvariantSpec extends FlatSpec with Matchers {

    val fixture = BlogFixtures(s"extropy-spec-${System.currentTimeMillis}")
    import fixture._

    behavior of "Ties"

    it should "resolve document locations" in {
        searchableTitleRule.tie.propagate(searchableTitleRule, DocumentLocation(posts,post1)) should be( DocumentLocation(posts,post1) )
        authorNameInPostRule.tie.propagate(authorNameInPostRule, DocumentLocation(posts,post1)) should be( IdLocation(users,"liz") )
        postCountInUserRule.tie.propagate(postCountInUserRule, DocumentLocation(users,userLiz)) should be( SimpleFilterLocation(posts,"authorId", "liz") )
/*
        commentCountInUserRule.tie.resolve(commentCountInUserRule, DocumentLocation(users,userJack)) should be(
            SubDocumentLocation(SimpleFilterLocation("comments.authorId", "jack"), SimpleFilterLocation(posts,"authorId", "jack"))
        )
*/
/*
        authorNameInCommentRule.tie.resolve(authorNameInCommentRule, SubDocumentLocation(DocumentLocation(posts,post2), DocumentLocation(comment1))) should be(
            IdLocation(users,"jack")
        )
*/
    }

    it should "resolve id locations" in {
        searchableTitleRule.tie.propagate(searchableTitleRule, IdLocation(posts,"post1")) should be( IdLocation(posts,"post1") )
        authorNameInPostRule.tie.propagate(authorNameInPostRule, IdLocation(posts,"post1")) should be(
            QueryLocation(users, IdLocation(posts,"post1"), "authorId")
        )
        postCountInUserRule.tie.propagate(postCountInUserRule, IdLocation(users,"liz")) should be( SimpleFilterLocation(posts,"authorId", "liz") )
/*
        commentCountInUserRule.tie.resolve(commentCountInUserRule, IdLocation(users,"jack")) should be(
            SubDocumentLocation(SimpleFilterLocation("comments.authorId", "jack"), SimpleFilterLocation(posts,"authorId", "jack"))
        )
*/
    }

    behavior of "Impact detection"

    it should "identify fields to monitor" in {
        searchableTitleRule.monitoredFields should be( Set( monitorPostsTitle ) )
        authorNameInPostRule.monitoredFields should be( Set( monitorPostsAuthorId, monitorUsersName ) )
        postCountInUserRule.monitoredFields should be( Set( monitorPostsAuthorId, monitorUsersId ) )
/*
        commentCountInUserRule.monitoredFields should be( Set( monitorPostsCommentsAuthorId, monitorUsersId ) )
        authorNameInCommentRule.monitoredFields should be(Set( monitorPostsCommentsAuthorId, monitorUsersName ))
*/
    }

    it should "identify monitor field in ModifiersUpdateChange" in {
        setNameOnUserLiz.impactedFields should be ( Set("name") )
    }

    it should "monitor inserts" taggedAs(Tag("a")) in {
        monitorUsersName.monitor(insertUserLiz) should be( Set(DocumentLocation(users,userLiz)) )
        monitorPostsTitle.monitor(insertUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(insertUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(insertUserLiz) should be ( 'empty )

        monitorUsersName.monitor(insertPost1) should be( 'empty )
        monitorPostsTitle.monitor(insertPost1) should be ( Set(DocumentLocation(posts,post1)) )
        monitorPostsAuthorId.monitor(insertPost1) should be ( Set(DocumentLocation(posts,post1)) )
/*
        monitorPostsCommentsAuthorId.monitor(insertPost1) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(insertPost2) should be (
            Set(SubDocumentLocation(DocumentLocation(posts,post2),SelectorLocation(MongoDBObject.empty) ) )
        )
*/
        monitorUsersName.monitor(insertNotUsers) should be( 'empty )
    }

    it should "monitor delete" in {
        monitorUsersName.monitor(deleteUserLiz) should be( Set(IdLocation(users,"liz")) )
        monitorPostsTitle.monitor(deleteUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(deleteUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(deleteUserLiz) should be ( 'empty )

        monitorUsersName.monitor(deletePost1) should be( 'empty )
        monitorPostsTitle.monitor(deletePost1) should be ( Set(IdLocation(posts,"post1")) )
        monitorPostsAuthorId.monitor(deletePost1) should be ( Set(IdLocation(posts,"post1")) )
//        monitorPostsCommentsAuthorId.monitor(deletePost1) should be ( Set(IdLocation(posts,"post1")) )
    }

    it should "monitor full body update" in {
        monitorUsersName.monitor(fbuUserLiz) should be( Set(IdLocation(users,"liz")) )
        monitorPostsTitle.monitor(fbuUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(fbuUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(fbuUserLiz) should be ( 'empty )

        monitorUsersName.monitor(fbuPost1) should be( 'empty )
        monitorPostsTitle.monitor(fbuPost1) should be ( Set(IdLocation(posts,"post1")) )
        monitorPostsAuthorId.monitor(fbuPost1) should be ( Set(IdLocation(posts,"post1")) )
//        monitorPostsCommentsAuthorId.monitor(fbuPost1) should be ( Set(IdLocation(posts,"post1")) )
    }

    it should "monitor modifiers update" in {
        monitorUsersName.monitor(setNameOnUserLiz) should be ( Set(IdLocation(users,"liz")) )
        monitorPostsTitle.monitor(setNameOnUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNameOnUserLiz) should be ( 'empty )
//        monitorPostsCommentsAuthorId.monitor(setNameOnUserLiz) should be ( 'empty )

        monitorUsersName.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsTitle.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )
//        monitorPostsCommentsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )
    }

    behavior of "a searchableTitleRule-like rule"

    it should "identify dirty set for inserts" in {
        searchableTitleRule.dirtiedSet( insertUserLiz ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( insertPost1 ) should be ( Set(DocumentLocation(posts,post1)) )
    }

    it should "identify dirty set for modifiers updates" in {
        searchableTitleRule.dirtiedSet( setTitleOnPost1 ) should be( Set(IdLocation(posts,"post1")) )
        searchableTitleRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        searchableTitleRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        searchableTitleRule.dirtiedSet( setAuthorIdOnPost1 ) should be( 'empty )
    }

    it should "identify dirty set for fbu updates" in {
        searchableTitleRule.dirtiedSet( fbuUserLiz ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( fbuPost1 ) should be ( Set(IdLocation(posts,"post1")) )
    }

    it should "identify dirty set for delete" in {
        searchableTitleRule.dirtiedSet( deleteUserLiz ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( deletePost1 ) should be ( Set(IdLocation(posts,"post1")) ) // TODO: empty would be better.
    }

    behavior of "a authorNameInPostRule rule"

    it should "identify dirty set for inserts" in {
        authorNameInPostRule.dirtiedSet( insertUserLiz ) should be ( Set(SimpleFilterLocation(posts,"authorId", "liz")) )
        authorNameInPostRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        authorNameInPostRule.dirtiedSet( insertPost1 ) should be ( Set(DocumentLocation(posts,post1)) )
    }

    it should "identify dirty set for modifiers updates" in {
        authorNameInPostRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        authorNameInPostRule.dirtiedSet( setNameOnUserLiz ) should be( Set(SimpleFilterLocation(posts,"authorId", "liz")) )
        authorNameInPostRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        authorNameInPostRule.dirtiedSet( setAuthorIdOnPost1 ) should be( Set(IdLocation(posts,"post1")) )
    }

    it should "identify dirty set for fbu updates" in {
        authorNameInPostRule.dirtiedSet( fbuUserLiz ) should be ( Set(SimpleFilterLocation(posts,"authorId", "liz")) )
        authorNameInPostRule.dirtiedSet( fbuPost1 ) should be ( Set(IdLocation(posts,"post1")) )
    }

    it should "identify dirty set for delete" in {
        authorNameInPostRule.dirtiedSet( deleteUserLiz ) should be ( Set(SimpleFilterLocation(posts,"authorId", "liz")) ) // TODO: not sure about this one
        authorNameInPostRule.dirtiedSet( deletePost1 ) should be ( Set(IdLocation(posts,"post1")) ) // TODO: empty should probably be better
    }

    behavior of "a postCountInUserRule rule"

    it should "identify dirty set for inserts" in {
        postCountInUserRule.dirtiedSet( insertUserLiz ) should be ( Set(DocumentLocation(users,userLiz)) )
        postCountInUserRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        postCountInUserRule.dirtiedSet( insertPost1 ) should be ( Set(
            ShakyLocation(QueryLocation(users, DocumentLocation(posts,post1), "authorId"))
        ))
    }

    it should "identify dirty set for modifiers updates" in {
        postCountInUserRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setAuthorIdOnPost1 ) should be( Set(
            ShakyLocation(QueryLocation(users, IdLocation(posts,"post1"), "authorId"))
        ))
    }

    it should "identify dirty set for fbu updates" in {
        postCountInUserRule.dirtiedSet( fbuUserLiz ) should be ( Set(IdLocation(users,"liz")) )
        postCountInUserRule.dirtiedSet( fbuPost1 ) should be ( Set(
            ShakyLocation(QueryLocation(users, IdLocation(posts,"post1"), "authorId"))
        ))
    }

    it should "identify dirty set for delete" in {
        postCountInUserRule.dirtiedSet( deleteUserLiz ) should be ( Set(IdLocation(users,"liz")) ) // TODO: optimize me
        postCountInUserRule.dirtiedSet( deletePost1 ) should be ( Set(
            ShakyLocation(QueryLocation(users, IdLocation(posts,"post1"), "authorId"))
        ))
    }

    behavior of "a commentCountInUserRule rule"

    it should "identify dirty set for inserts" in {
        commentCountInUserRule.dirtiedSet( insertUserLiz ) should be ( Set(DocumentLocation(users,userLiz)) )
        commentCountInUserRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        commentCountInUserRule.dirtiedSet( insertPost1 ) should be ( 'empty )
        commentCountInUserRule.dirtiedSet( insertPost2 ) should be ( Set(
            ShakyLocation(QueryLocation(users, NestedLocation(DocumentLocation(posts,post2), "comments"), "authorId"))
        ))
    }

    it should "identify dirty set for modifiers updates" in {
        commentCountInUserRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        commentCountInUserRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        commentCountInUserRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        commentCountInUserRule.dirtiedSet( setAuthorIdOnPost1 ) should be( 'empty )
    }

    it should "identify dirty set for fbu updates" in {
        commentCountInUserRule.dirtiedSet( fbuUserLiz ) should be ( Set(IdLocation(users,"liz")) )
        commentCountInUserRule.dirtiedSet( fbuPost1 ) should be ( Set(
            ShakyLocation(QueryLocation(users, IdLocation(posts,"post1"), "authorId"))
        ))
    }

    it should "identify dirty set for delete" in {
        commentCountInUserRule.dirtiedSet( deleteUserLiz ) should be ( Set(IdLocation(users,"liz")) ) // TODO: optimize me
        commentCountInUserRule.dirtiedSet( deletePost1 ) should be ( Set(
            ShakyLocation(QueryLocation(users, IdLocation(posts,"post1"), "authorId"))
        ))
    }

/*
    behavior of "a authorNameInComment rule"

    it should "identify dirty set for inserts" taggedAs(Tag("c")) in {
pending
        authorNameInCommentRule.dirtiedSet( insertUserLiz ) should be ( Set(
            SubDocumentLocation(SimpleFilterLocation("comments.authorId", "liz"), SimpleFilterLocation(posts,"authorId","liz")))
        )
        authorNameInCommentRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        authorNameInCommentRule.dirtiedSet( insertPost1 ) should be ( Set(
            QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId"),
            SnapshotLocation(QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId") ) ) )
    }

    it should "identify dirty set for modifiers updates" in {
pending
        authorNameInCommentRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        authorNameInCommentRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        authorNameInCommentRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        authorNameInCommentRule.dirtiedSet( setAuthorIdOnPost1 ) should be( Set(
            QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId"),
            SnapshotLocation(QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId") ) ) )
    }

    it should "identify dirty set for fbu updates" in {
pending
        authorNameInCommentRule.dirtiedSet( fbuUserLiz ) should be ( Set(IdLocation(users,"liz")) )
        authorNameInCommentRule.dirtiedSet( fbuPost1 ) should be ( Set(
            QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId"),
            SnapshotLocation(QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId") ) ) )
    }

    it should "identify dirty set for delete" in {
pending
        authorNameInCommentRule.dirtiedSet( deleteUserLiz ) should be ( Set(IdLocation(users,"liz")) ) // TODO: optimize me
        authorNameInCommentRule.dirtiedSet( deletePost1 ) should be ( Set(
            QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId"),
            SnapshotLocation(QueryLocation(SubCollectionContainer(s"$dbName.posts", "comments"), IdLocation(posts,"post1"), "authorId") ) ) )
    }
*/

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
