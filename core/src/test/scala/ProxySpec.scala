package org.zoy.kali.extropy.proxy

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import org.zoy.kali.extropy._
import org.zoy.kali.extropy.mongo._

import com.mongodb.casbah.Imports._

class ProxySpec extends TestKit(ActorSystem()) with ImplicitSender
  with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll with MongodbTemporary {

    behavior of "An extropy proxy"

    def withExtropy(testCode:((BaseExtropyContext,String) => Any)) {
        val id = System.currentTimeMillis.toString
        val dbName = s"extropy-spec-$id"
        val extropy = Extropy(mongoBackendClient(dbName))
        try {
            extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule(s"$id.users", "name", "normName")) )
            testCode(extropy, id)
        } finally {
            mongoBackendClient.dropDatabase(dbName)
        }
    }

    it should "leave read messages alone" in withExtropy { (extropy,id) =>
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        val original = TargettedMessage(Server,
                    CraftedMessage(0, 0, OpQuery(0, s"$id.users", 12, 12, MongoDBObject("name" -> "Kali"), None))
                )
        proxy ! original
        val transformed = expectMsgClass(classOf[TargettedMessage])
        transformed should be(original)
    }

    it should "leave messages on an arbitrary collection alone" in withExtropy { (extropy,id) =>
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        val original = TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, s"$id.not-users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        proxy ! original
        val transformed = expectMsgClass(classOf[TargettedMessage])
        transformed should be(original)
    }

    it should "transform messages on the right collection" in withExtropy { (extropy,id) =>
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, s"$id.users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        transformed.direction should be(Server)
        val op:OpInsert = transformed.message.op.asInstanceOf[OpInsert]
        op.fullCollectionName should be(s"$id.users")
        op.documents.size should be(1)
        op.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    override def afterAll { TestKit.shutdownActorSystem(system) ; super.afterAll }
}
