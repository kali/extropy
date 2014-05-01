package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class InvariantSpec extends FlatSpec with ShouldMatchers {

    import BlogFixtures._

    behavior of "Value computation internals"

    it should "follow ties for document locations" in {
        searchableTitleRule.tie.resolve(searchableTitleRule, DocumentLocation(post1)) should be( DocumentLocation(post1) )
        authorNameInPostRule.tie.resolve(authorNameInPostRule, DocumentLocation(post1)) should be( IdLocation("liz") )
        postCountInUserRule.tie.resolve(postCountInUserRule, DocumentLocation(userLiz)) should be( SelectorLocation(MongoDBObject("authorId" -> "liz")) )
    }

    it should "follow ties for id locations" in {
        searchableTitleRule.tie.resolve(searchableTitleRule, IdLocation("post1")) should be( IdLocation("post1") )
        authorNameInPostRule.tie.resolve(authorNameInPostRule, IdLocation("post1")) should be( QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId") )
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
        monitorUsersName.monitor(insertUsers) should be( Set(DocumentLocation(userLiz),DocumentLocation(userJack)) )
        monitorPostsTitle.monitor(insertUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(insertUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(insertUsers) should be ( 'empty )

        monitorUsersName.monitor(insertPosts) should be( 'empty )
        monitorPostsTitle.monitor(insertPosts) should be ( Set(DocumentLocation(post1), DocumentLocation(post2)) )
        monitorPostsAuthorId.monitor(insertPosts) should be ( Set(DocumentLocation(post1), DocumentLocation(post2)) )
        //monitorPostsCommentsAuthorId.monitor(insertPosts) should be ( Set(DocumentLocation(post2)) ) // FIXME
        monitorPostsCommentsAuthorId.monitor(insertPosts) should be ( Set(DocumentLocation(post1), DocumentLocation(post2)) )

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
        searchableTitleRule.dirtiedSet( insertUsers ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        searchableTitleRule.dirtiedSet( insertPost1 ) should be ( Set(DocumentLocation(post1)) )
        searchableTitleRule.dirtiedSet( insertPosts ) should be ( Set(DocumentLocation(post1),DocumentLocation(post2)) )
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
        authorNameInPostRule.dirtiedSet( insertUsers ) should be ( Set( SelectorLocation(MongoDBObject("authorId" -> "liz")),
                                                                        SelectorLocation(MongoDBObject("authorId" -> "jack"))))
        authorNameInPostRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        authorNameInPostRule.dirtiedSet( insertPost1 ) should be ( Set(DocumentLocation(post1)) )
        authorNameInPostRule.dirtiedSet( insertPosts ) should be ( Set(DocumentLocation(post1),DocumentLocation(post2)) )
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
        postCountInUserRule.dirtiedSet( insertUsers ) should be ( Set(DocumentLocation(userLiz), DocumentLocation(userJack), DocumentLocation(userCatLady)))
        postCountInUserRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        postCountInUserRule.dirtiedSet( insertPost1 ) should be ( Set(
            QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId") ) ) )
        postCountInUserRule.dirtiedSet( insertPosts ) should be ( Set(
            QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId")),
            QueryLocation(CollectionContainer("blog.posts"), IdLocation("post2"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer("blog.posts"), IdLocation("post2"), "authorId"))  ) )
    }

    it should "identify dirty set for modifiers updates" in {
        postCountInUserRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        postCountInUserRule.dirtiedSet( setAuthorIdOnPost1 ) should be( Set(
            QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId") ) ) )
    }

    it should "identify dirty set for fbu updates" in {
        postCountInUserRule.dirtiedSet( fbuUserLiz ) should be ( Set(IdLocation("liz")) )
        postCountInUserRule.dirtiedSet( fbuPost1 ) should be ( Set(
            QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId") ) ) )
    }

    it should "identify dirty set for delete" in {
        postCountInUserRule.dirtiedSet( deleteUserLiz ) should be ( Set(IdLocation("liz")) ) // TODO: optimize me
        postCountInUserRule.dirtiedSet( deletePost1 ) should be ( Set(
            QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId"),
            SnapshotLocation(QueryLocation(CollectionContainer("blog.posts"), IdLocation("post1"), "authorId") ) ) )
    }

    /*
    it should "back propagate locations detected by processor" in {
        val same = SameDocumentTie(CollectionContainer("blog.posts"))
        same.backPropagate(IdLocation( "post1"))) should be (IdLocation( "post1")))

        // when user information change, I need to flag dirty all posts with matching authorId
        val follow = FollowKeyTie("blog.users", "authorId")
        follow.backPropagate(IdLocation( "liz"))) should be (SelectorLocation(MongoDBObject("authorId" -> "liz")))

        // when authorId in a post change, I need to flag dirty users which id was the previous value, and the one with new value (post count)
        val reverseTop = ReverseKeyTie(CollectionContainer("blog.posts"), "authorId")
        reverseTop.backPropagate(IdLocation( "post1"))) should be (BeforeAndAfterIdLocation(CollectionContainer("blog.posts"), MongoDBObject("_id" -> "post1"), "authorId"))

        // when authorId in a comment change, I need to flag dirty users which id was the previous value, and the one with new value (comment count)
        val reverseSub = ReverseKeyTie(SubCollectionContainer("blog.posts","comments"), "authorId")
        reverseSub.backPropagate(SelectorLocation(MongoDBObject("comments._id" -> "comment1"))) should be (BeforeAndAfterIdLocation(SubCollectionContainer("blog.posts", "comments"), MongoDBObject("comments._id" -> "comment1"), "authorId"))

        // when user information change, I need to flag dirty all posts with matching authorId
/*
        FollowKeyTie("blog.users", "authorId"),
*/
    }
*/

/*
    it should "detected monitored locations on insert" in {
    }
*/

}
