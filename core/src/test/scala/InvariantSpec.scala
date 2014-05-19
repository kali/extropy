package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import com.mongodb.casbah.Imports._

import mongoutils.BSONObjectConversions._

import org.json4s._
import org.json4s.native.JsonMethods._

class InvariantSpec extends FlatSpec with Matchers {

    val fixture = BlogFixtures(s"extropy-spec-${System.currentTimeMillis}")
    import fixture._

    behavior of "Ties"

    it should "resolve document locations" in {
        searchableTitleRule.tie.propagate(searchableTitleRule, DocumentLocation(posts,post1)) should be(
            DocumentLocation(posts,post1)
        )
        authorNameInPostRule.tie.propagate(authorNameInPostRule, DocumentLocation(posts,post1)) should be(
            IdLocation(users,"liz")
        )
        postCountInUserRule.tie.propagate(postCountInUserRule, DocumentLocation(users,userLiz)) should be(
            SimpleFilterLocation(posts,"authorId", "liz")
        )
        commentCountInUserRule.tie.propagate(commentCountInUserRule, DocumentLocation(users,userJack)) should be(
            SimpleNestedLocation(comments, "authorId", "jack")
        )
        authorNameInCommentRule.tie.propagate(authorNameInCommentRule,
            NestedDataDocumentLocation(comments, post2, comment1)) should be(
                IdLocation(users,"jack")
        )
    }

    it should "resolve id locations" in {
        searchableTitleRule.tie.propagate(searchableTitleRule, IdLocation(posts,"post1")) should be(
            IdLocation(posts,"post1")
        )
        authorNameInPostRule.tie.propagate(authorNameInPostRule, IdLocation(posts,"post1")) should be(
            QueryLocation(users, IdLocation(posts,"post1"), "authorId")
        )
        postCountInUserRule.tie.propagate(postCountInUserRule, IdLocation(users,"liz")) should be(
            SimpleFilterLocation(posts,"authorId", "liz")
        )
        commentCountInUserRule.tie.propagate(commentCountInUserRule, IdLocation(users,"jack")) should be(
            SimpleNestedLocation(comments, "authorId", "jack")
        )
        authorNameInCommentRule.tie.propagate(authorNameInPostRule,
                NestedIdLocation(comments, "post1", IdSubDocumentLocationFilter("comment1"))) should be(
            QueryLocation(users, NestedIdLocation(comments, "post1", IdSubDocumentLocationFilter("comment1")), "authorId")
        )
    }

    behavior of "Impact detection"

    it should "identify fields to monitor" in {
        searchableTitleRule.monitoredFields should be( Set( monitorPostsTitle, monitorPostsId ) )
        authorNameInPostRule.monitoredFields should be( Set( monitorPostsAuthorId, monitorUsersName, monitorPostsId ) )
        postCountInUserRule.monitoredFields should be( Set( monitorPostsAuthorId, monitorUsersId ) )
        commentCountInUserRule.monitoredFields should be( Set( monitorPostsCommentsAuthorId, monitorUsersId ) )
        authorNameInCommentRule.monitoredFields should be(Set( monitorPostsCommentsAuthorId, monitorUsersName, monitorCommentsId ))
        commentCountInPostRule.monitoredFields should be( Set( monitorCommentsId, monitorPostsId ) )
    }

    it should "identify monitor field in ModifiersUpdateChange" taggedAs(Tag("r")) in {
        setNameOnUserLiz.impactedFields should be ( Set("name") )
        setAuthorIdOnPost1.impactedFields should be ( Set("authorId") )
        setAuthorIdOnComment1.impactedFields should be ( Set("comments.$.authorId") )
    }

    it should "monitor inserts" taggedAs(Tag("a")) in {
        monitorUsersName.monitor(insertUserLiz) should be( Set(DocumentLocation(users,userLiz)) )
        monitorPostsTitle.monitor(insertUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(insertUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(insertUserLiz) should be ( 'empty )

        monitorUsersName.monitor(insertPost1) should be( 'empty )
        monitorPostsTitle.monitor(insertPost1) should be ( Set(DocumentLocation(posts,post1)) )
        monitorPostsAuthorId.monitor(insertPost1) should be ( Set(DocumentLocation(posts,post1)) )
        monitorPostsCommentsAuthorId.monitor(insertPost1) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(insertPost2) should be (
            Set(NestedDocumentLocation(comments, post2, AnySubDocumentLocationFilter))
        )
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
        monitorPostsCommentsAuthorId.monitor(deletePost1) should be ( Set(
            NestedIdLocation(comments, "post1", AnySubDocumentLocationFilter)
        ))
    }

    it should "monitor full body update" in {
        monitorUsersName.monitor(fbuUserLiz) should be( Set(IdLocation(users,"liz")) )
        monitorPostsTitle.monitor(fbuUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(fbuUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(fbuUserLiz) should be ( 'empty )

        monitorUsersName.monitor(fbuPost1) should be( 'empty )
        monitorPostsTitle.monitor(fbuPost1) should be ( Set(IdLocation(posts,"post1")) )
        monitorPostsAuthorId.monitor(fbuPost1) should be ( Set(IdLocation(posts,"post1")) )
        monitorPostsCommentsAuthorId.monitor(fbuPost1) should be ( Set(
            NestedIdLocation(comments, "post1", AnySubDocumentLocationFilter)
        ))
    }

    it should "monitor modifiers update" taggedAs(Tag("r")) in {
        monitorUsersName.monitor(setNameOnUserLiz) should be ( Set(IdLocation(users,"liz")) )
        monitorPostsTitle.monitor(setNameOnUserLiz) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNameOnUserLiz) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(setNameOnUserLiz) should be ( 'empty )

        monitorUsersName.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsTitle.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(setNotNameOnUsers) should be ( 'empty )

        monitorUsersName.monitor(setAuthorIdOnPost1) should be ( 'empty )
        monitorPostsTitle.monitor(setAuthorIdOnPost1) should be ( 'empty )
        monitorPostsAuthorId.monitor(setAuthorIdOnPost1) should be ( Set( IdLocation(posts, "post1") ))
        monitorPostsCommentsAuthorId.monitor(setAuthorIdOnPost1) should be ( 'empty )

        monitorUsersName.monitor(setAuthorIdOnComment1) should be ( 'empty )
        monitorPostsTitle.monitor(setAuthorIdOnComment1) should be ( 'empty )
        monitorPostsAuthorId.monitor(setAuthorIdOnComment1) should be ( 'empty )
        monitorPostsCommentsAuthorId.monitor(setAuthorIdOnComment1) should be ( Set(
            NestedSelectorLocation(comments, MongoDBObject("comments._id" -> "comment1"),
                IdSubDocumentLocationFilter("comment1"))
        ))
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
        commentCountInUserRule.dirtiedSet( insertPost2 ) should be (Set(
            ShakyLocation(QueryLocation(users, NestedDocumentLocation(comments,post2,AnySubDocumentLocationFilter), "authorId")) // FIXME not shaky
        ))
    }

    it should "identify dirty set for modifiers updates" in {
        commentCountInUserRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        commentCountInUserRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        commentCountInUserRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        commentCountInUserRule.dirtiedSet( setAuthorIdOnPost1 ) should be( 'empty )
        commentCountInUserRule.dirtiedSet( setAuthorIdOnComment1 ) should be( Set(
            ShakyLocation(QueryLocation(users, NestedSelectorLocation(comments, setAuthorIdOnComment1.selector, IdSubDocumentLocationFilter("comment1")), "authorId"))
        ))
        commentCountInUserRule.dirtiedSet( setAuthorIdOnCommentsNestedSel ) should be( Set(
            ShakyLocation(QueryLocation(users, NestedSelectorLocation(comments, setAuthorIdOnCommentsNestedSel.selector, AnySubDocumentLocationFilter), "authorId")) // TODO:optimize me
        ))
    }

    it should "identify dirty set for fbu updates" in {
        commentCountInUserRule.dirtiedSet( fbuUserLiz ) should be ( Set(IdLocation(users,"liz")) )
        commentCountInUserRule.dirtiedSet( fbuPost1 ) should be ( Set(
            ShakyLocation(QueryLocation(users, NestedIdLocation(comments,"post1",AnySubDocumentLocationFilter), "authorId"))
        ))
    }

    it should "identify dirty set for delete" in {
        commentCountInUserRule.dirtiedSet( deleteUserLiz ) should be ( Set(IdLocation(users,"liz")) ) // TODO: optimize me
        commentCountInUserRule.dirtiedSet( deletePost1 ) should be ( Set(
            ShakyLocation(QueryLocation(users, NestedIdLocation(comments,"post1",AnySubDocumentLocationFilter), "authorId"))
        ))
    }

    behavior of "a authorNameInComment rule"

    it should "identify dirty set for inserts" taggedAs(Tag("c")) in {
        authorNameInCommentRule.dirtiedSet( insertUserLiz ) should be ( Set(
            SimpleNestedLocation(comments, "authorId", "liz")
        ))
        authorNameInCommentRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        authorNameInCommentRule.dirtiedSet( insertPost1 ) should be ( 'empty )
        authorNameInCommentRule.dirtiedSet( insertPost2 ) should be ( Set(
            NestedDocumentLocation(comments, post2, AnySubDocumentLocationFilter)
        ))
    }

    it should "identify dirty set for modifiers updates" in {
        authorNameInCommentRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        authorNameInCommentRule.dirtiedSet( setNameOnUserLiz ) should be( Set(
            SimpleNestedLocation(comments, "authorId", "liz")
        ))
        authorNameInCommentRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        authorNameInCommentRule.dirtiedSet( setAuthorIdOnPost1 ) should be( 'empty )
        authorNameInCommentRule.dirtiedSet( setAuthorIdOnComment1 ) should be( Set(
            NestedSelectorLocation(comments, setAuthorIdOnComment1.selector, IdSubDocumentLocationFilter("comment1"))
        ))
        authorNameInCommentRule.dirtiedSet( setAuthorIdOnCommentsNestedSel ) should be( Set(
            NestedSelectorLocation(comments, setAuthorIdOnCommentsNestedSel.selector, AnySubDocumentLocationFilter)
        ))
    }

    it should "identify dirty set for fbu updates" in {
        authorNameInCommentRule.dirtiedSet( fbuUserLiz ) should be ( Set(
            SimpleNestedLocation(comments, "authorId", "liz")
        ))
        authorNameInCommentRule.dirtiedSet( fbuPost1 ) should be ( Set(
            NestedIdLocation(comments, "post1", AnySubDocumentLocationFilter)
        ))
        authorNameInCommentRule.dirtiedSet( fbuPost2 ) should be ( Set(
            NestedIdLocation(comments, "post2", AnySubDocumentLocationFilter)
        ))
    }

    it should "identify dirty set for delete" in {
        authorNameInCommentRule.dirtiedSet( deleteUserLiz ) should be ( Set(
            SimpleNestedLocation(comments, "authorId", "liz")
        ))
        authorNameInCommentRule.dirtiedSet( deletePost1 ) should be ( Set(
            NestedIdLocation(comments, "post1", AnySubDocumentLocationFilter) // FIXME: empty
        ))
    }

    behavior of "a commentCountInPost rule"

    it should "identify dirty set for inserts" taggedAs(Tag("c")) in {
        commentCountInPostRule.dirtiedSet( insertUserLiz ) should be ( 'empty )
        commentCountInPostRule.dirtiedSet( insertNotUsers ) should be ( 'empty )
        commentCountInPostRule.dirtiedSet( insertPost1 ) should be ( Set( DocumentLocation(posts, post1) ) )
        commentCountInPostRule.dirtiedSet( insertPost2 ) should be ( Set( DocumentLocation(posts, post2) ) )
    }

    it should "identify dirty set for modifiers updates" in {
        commentCountInPostRule.dirtiedSet( setTitleOnPost1 ) should be( 'empty )
        commentCountInPostRule.dirtiedSet( setNameOnUserLiz ) should be( 'empty )
        commentCountInPostRule.dirtiedSet( setNotNameOnUsers ) should be( 'empty )
        commentCountInPostRule.dirtiedSet( setAuthorIdOnPost1 ) should be( 'empty )
        commentCountInPostRule.dirtiedSet( setAuthorIdOnComment1 ) should be( 'empty )
        commentCountInPostRule.dirtiedSet( setAuthorIdOnCommentsNestedSel ) should be( 'empty )
    }

    it should "identify dirty set for fbu updates" in {
        commentCountInPostRule.dirtiedSet( fbuUserLiz ) should be ( 'empty )
        commentCountInPostRule.dirtiedSet( fbuPost1 ) should be ( Set(
            IdLocation(posts, "post1")
        ))
        commentCountInPostRule.dirtiedSet( fbuPost2 ) should be ( Set(
            IdLocation(posts, "post2")
        ))
    }

    it should "identify dirty set for delete" in {
        commentCountInPostRule.dirtiedSet( deleteUserLiz ) should be ( 'empty )
        commentCountInPostRule.dirtiedSet( deletePost1 ) should be ( Set(
            IdLocation(posts, "post1") // FIXME: empty
        ))
    }

}
