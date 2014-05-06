package org.zoy.kali.extropy.proxy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class ProxySpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll with MongodbTemporary {

    def withExtropy(testCode:((BaseExtropyContext,BlogFixtures) => Any)) {
        val now = System.currentTimeMillis
        val payloadDbName = s"extropy-spec-payload-$now"
        val extropyDbName = s"extropy-spec-internal-$now"
        val fixture = BlogFixtures(payloadDbName)
        val extropy = ExtropyContext(mongoBackendClient(extropyDbName), mongoBackendClient)
        fixture.allRules.foreach( rule => extropy.invariantDAO.salat.insert( Invariant(rule) ) )
        try {
            testCode(extropy, fixture)
        } finally {
            mongoBackendClient.dropDatabase(payloadDbName)
            mongoBackendClient.dropDatabase(extropyDbName)
        }
    }

    behavior of "A synchronous proxy"

    it should "deal with various inserts" in withExtropy { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        Seq(    Seq(insertPost1,insertPost2,insertUserLiz,insertUserJack),
                Seq(insertPost1,insertPost2,insertUsers),
                Seq(insertPosts,insertUsers)
        ).foreach { ops =>
            ops.permutations.foreach { perm =>
                Seq("users", "posts").foreach( extropy.payloadMongo(dbName)(_).remove(MongoDBObject.empty) )
                extropy.payloadMongo(dbName)("users").remove(MongoDBObject.empty)
                perm.foreach { op =>
                    proxy.doChange(op)
                    allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
                }
            }
        }
    }

    it should "deal with updates" in withExtropy { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        proxy.doChange(insertUsers)
        proxy.doChange(insertPosts)
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
    }

    it should "deal with deletes" in withExtropy { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        proxy.doChange(insertUsers)
        proxy.doChange(insertPosts)
        proxy.doChange(deletePost1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        proxy.doChange(deleteUserLiz)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
    }

    it should "leave messages on an arbitrary collection alone" in withExtropy { (extropy,id) =>
        pending
/*
        val proxy = ExtropyProxy(extropy)
        val original = InsertChange(s"$id.not-users", Stream(MongoDBObject("name" -> "Kali")))
        val transformed = proxy.processChange(original)
        transformed should be(original)
*/
    }

    it should "transform messages on the right collection" in withExtropy { (extropy,id) =>
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
