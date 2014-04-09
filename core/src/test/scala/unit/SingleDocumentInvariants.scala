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

    var extropy:Extropy = null
    override def beforeAll {
        super.beforeAll
        extropy = Extropy(s"mongodb://localhost:$mongoBackendPort")
        extropy.invariantDAO.save(
            StringNormalizationInvariant(new ObjectId(), "test.users", "name", "normName")
        )
    }

    it should "deal with insert" in {
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, "test.users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpInsert = transformed.message.op.asInstanceOf[OpInsert]
        op.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with full body update" in {
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpUpdate(0, "test.users", 0, MongoDBObject(), MongoDBObject("name" -> "Kali")))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpUpdate = transformed.message.op.asInstanceOf[OpUpdate]
        op.update should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with modifier update" in {
        val proxy = system.actorOf(ExtropyProxy.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpUpdate(0, "test.users", 0, MongoDBObject(),
                        MongoDBObject("$set" -> MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpUpdate = transformed.message.op.asInstanceOf[OpUpdate]
        op.update should be(MongoDBObject("$set" -> MongoDBObject("name" -> "Kali", "normName" -> "kali")))
    }

    override def afterAll { super.afterAll; TestKit.shutdownActorSystem(system) }
}
