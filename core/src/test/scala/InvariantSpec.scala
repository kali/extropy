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

    behavior of "Rule"

    val searchableTitleRule = Rule( CollectionContainer("blog.posts"),
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

    it should "identify fields to monitor" in {
        searchableTitleRule.monitoredFields should be(
            Set(MonitoredField(CollectionContainer("blog.posts"), "title"))
        )
        authorNameInPostRule.monitoredFields should be(Set(
            MonitoredField(CollectionContainer("blog.posts"), "authorId"),
            MonitoredField(CollectionContainer("blog.users"), "name")
        ))
        postCountInUserRule.monitoredFields should be(Set(
            MonitoredField(CollectionContainer("blog.posts"), "authorId")
        ))
        commentCountInUserRule.monitoredFields should be(Set(
            MonitoredField(SubCollectionContainer("blog.posts","comments"), "authorId")
        ))
        authorNameInComment.monitoredFields should be(Set(
            MonitoredField(SubCollectionContainer("blog.posts","comments"), "authorId"),
            MonitoredField(CollectionContainer("blog.users"), "name")
        ))
    }
}
