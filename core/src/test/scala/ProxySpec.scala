package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class ProxySpec extends FlatSpec with Matchers with BeforeAndAfterAll with ExtropyFixtures {

    behavior of "A synchronous proxy"

    it should "deal with various inserts" taggedAs(Tag("r")) in withExtropyAndBlog { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        Seq(insertPost1,insertPost2,insertUserLiz,insertUserJack).foreach { op =>
            Seq("users", "posts").foreach( extropy.payloadMongo(dbName)(_).remove(MongoDBObject.empty) )
            proxy.doChange(op)
            withClue( s"after: $op:\n" ) {
                allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
            }
        }
    }

    it should "deal with various inserts permutations" in withExtropyAndBlog { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        Seq(insertPost1,insertPost2,insertUserLiz,insertUserJack).permutations.foreach { perm =>
            Seq("users", "posts").foreach( extropy.payloadMongo(dbName)(_).remove(MongoDBObject.empty) )
            perm.foreach { op =>
                proxy.doChange(op)
                withClue( s"on permutation:\n${perm.map ( " -> " + _ + "\n").mkString }\n at step:\n$op" ) {
                    allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
                }
            }
        }
    }

    it should "deal with updates" in withExtropyAndBlog { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        Array(insertUserJack, insertUserLiz, insertUserCatLady, insertPost1, insertPost2).foreach( proxy.doChange(_) )
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(setNameOnUserLiz)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(setTitleOnPost1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(setAuthorIdOnPost1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(setNotNameOnUsers)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(fbuUserLiz)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(fbuPost1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(setAuthorIdOnComment1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(setAuthorIdOnCommentsNestedSel)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(pushCommentInPost1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
    }

    it should "deal with deletes" in withExtropyAndBlog { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        Array(insertUserJack, insertUserLiz, insertUserCatLady, insertPost1, insertPost2).foreach( proxy.doChange(_) )
        proxy.doChange(deletePost1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(deleteUserLiz)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
    }

    it should "leave messages on an arbitrary collection alone" in withExtropyAndBlog { (extropy,id) =>
        pending
/*
        val proxy = ExtropyProxy(extropy)
        val original = InsertChange(s"$id.not-users", Stream(MongoDBObject("name" -> "Kali")))
        val transformed = proxy.processChange(original)
        transformed should be(original)
*/
    }

    it should "transform messages on the right collection" in withExtropyAndBlog { (extropy,id) =>
        pending
/*
        val proxy = ExtropyProxy(extropy)
        val original = InsertChange(s"$id.users", Stream(MongoDBObject("name" -> "Kali")))
        val result = proxy.processChange(original)
        result should be (a [InsertChange])
        val transformed = result.asInstanceOf[InsertChange]
        transformed.writtenCollection should be(s"$id.users")
        transformed.documents.size should be(1)
        transformed.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
*/
    }
}
