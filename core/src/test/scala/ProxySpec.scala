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

    it should "deal with insert" in withExtropy { (extropy, fixture) =>
        val proxy = SyncProxy(extropy)
        import fixture._
        proxy.doChange(InsertChange(s"$dbName.posts", Stream(post1)))
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
