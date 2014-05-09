package org.zoy.kali.extropy

import scala.concurrent.duration._

import org.scalatest._

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import org.zoy.kali.extropy._
import org.zoy.kali.extropy.mongo._

import com.mongodb.casbah.Imports._

class SameDocumentInvariantSpec extends TestKit(ActorSystem()) with ImplicitSender
    with FlatSpecLike with Matchers with BeforeAndAfterAll with ExtropyFixtures {

    behavior of "A same-document invariant"

    it should "deal with insert" in withExtropyAndBlog { (extropy,blog) =>
        pending
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxyActor.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, "test.users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpInsert = transformed.message.op.asInstanceOf[OpInsert]
        op.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with full body update" in withExtropyAndBlog { (extropy,blog) =>
        pending
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxyActor.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpUpdate(0, "test.users", 0, MongoDBObject(), MongoDBObject("name" -> "Kali")))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpUpdate = transformed.message.op.asInstanceOf[OpUpdate]
        op.update should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with modifier update" in withExtropyAndBlog { (extropy,blog) =>
        pending
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxyActor.props(extropy))
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
