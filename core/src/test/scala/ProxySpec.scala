package org.zoy.kali.extropy.proxy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import org.zoy.kali.extropy._

import com.mongodb.casbah.Imports._

class ProxySpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll with MongodbTemporary {

    behavior of "An extropy proxy"

    def withExtropy(testCode:((BaseExtropyContext,String) => Any)) {
        val id = System.currentTimeMillis.toString
        val dbName = s"extropy-spec-$id"
        val extropy = ExtropyContext(mongoBackendClient(dbName), mongoBackendClient)
        try {
            extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule(s"$id.users", "name", "normName")) )
            testCode(extropy, id)
        } finally {
            mongoBackendClient.dropDatabase(dbName)
        }
    }

    it should "leave messages on an arbitrary collection alone" in withExtropy { (extropy,id) =>
        pending
        val proxy = ExtropyProxy(extropy)
        val original = InsertChange(s"$id.not-users", Stream(MongoDBObject("name" -> "Kali")))
/*
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
