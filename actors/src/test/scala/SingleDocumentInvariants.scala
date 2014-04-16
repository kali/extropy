package org.zoy.kali.extropy

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
        val extropy = Extropy(mongoBackendClient(dbName), mongoBackendClient)
        try {
            testCode(id, extropy)
        } finally {
        }
    }

    it should "deal with insert" in withExtropy { (id,extropy) =>
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxyActor.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, "test.users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpInsert = transformed.message.op.asInstanceOf[OpInsert]
        op.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with full body update" in withExtropy { (id, extropy) =>
        extropy.invariantDAO.salat.save( Invariant(StringNormalizationRule("test.users", "name", "normName")) )
        val proxy = system.actorOf(ExtropyProxyActor.props(extropy))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpUpdate(0, "test.users", 0, MongoDBObject(), MongoDBObject("name" -> "Kali")))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        val op:OpUpdate = transformed.message.op.asInstanceOf[OpUpdate]
        op.update should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

    it should "deal with modifier update" in withExtropy { (id, extropy) =>
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

    behavior of "A SingleDocumentRule rule"

    it should "fix all existing documents when activeSync is called" taggedAs(Tag("bla")) in withExtropy { (id, extropy) =>
        val rule = StringNormalizationRule(s"db-$id.users", "name", "normName")
        (1 to 100).foreach { i =>
            extropy.payloadMongo(s"db-$id")("users").save( MongoDBObject("name" -> s"Kali-$i") )
        }
        rule.activeSync(extropy)
        extropy.payloadMongo(s"db-$id")("users").foreach { dbo =>
            dbo.keys should contain("normName")
            dbo.keys should contain("name")
            dbo("normName") should be(dbo("name").asInstanceOf[String].toLowerCase)
        }
    }

    override def afterAll { TestKit.shutdownActorSystem(system) ; super.afterAll }
}
