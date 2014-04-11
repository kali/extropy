package org.zoy.kali.extropy.unit

import scala.concurrent.duration._

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import org.zoy.kali.extropy._
import org.zoy.kali.extropy.mongo._

import com.mongodb.casbah.Imports._

class SameDocumentInvariantSpec extends TestKit(ActorSystem()) with ImplicitSender
    with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll with MongodbTemporary {

    behavior of "A same-document invariant"

    def withExtropy(testCode:((String, BaseExtropyContext) => Any)) {
        val id = System.currentTimeMillis.toString
        val dbName = s"extropy-spec-$id"
        val extropy = Extropy(mongoBackendClient(dbName))
        try {
            testCode(id, extropy)
        } finally {
        }
    }

    it should "deal with insert" in withExtropy { (id,extropy) =>
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, "test.users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpInsert = transformed.message.op.asInstanceOf[OpInsert]
        op.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with full body update" in withExtropy { (id, extropy) =>
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpUpdate(0, "test.users", 0, MongoDBObject(), MongoDBObject("name" -> "Kali")))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpUpdate = transformed.message.op.asInstanceOf[OpUpdate]
        op.update should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with modifier update" in withExtropy { (id, extropy) =>
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpUpdate(0, "test.users", 0, MongoDBObject(),
                        MongoDBObject("$set" -> MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpUpdate = transformed.message.op.asInstanceOf[OpUpdate]
        op.update should be(MongoDBObject("$set" -> MongoDBObject("name" -> "Kali", "normName" -> "kali")))
    }

    override def afterAll { TestKit.shutdownActorSystem(system) ; super.afterAll }
}
