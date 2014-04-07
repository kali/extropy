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
  with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll {

    behavior of "An extropy proxy"

    val invariants = List(StringNormalizationInvariant(new ObjectId(), "test.users", "name", "normName"))

    it should "leave read messages alone" in {
        val proxy = system.actorOf(ExtropyProxy.props(invariants))
        val original = TargettedMessage(Server,
                    CraftedMessage(0, 0, OpQuery(0, "test.users", 12, 12, MongoDBObject("name" -> "Kali"), None))
                )
        proxy ! original
        val transformed = expectMsgClass(classOf[TargettedMessage])
        transformed should be(original)
    }

    it should "leave messages on an arbitrary collection alone" in {
        val proxy = system.actorOf(ExtropyProxy.props(invariants))
        val original = TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, "test.not-users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        proxy ! original
        val transformed = expectMsgClass(classOf[TargettedMessage])
        transformed should be(original)
    }

    it should "transform messages on the right collection" in {
        val proxy = system.actorOf(ExtropyProxy.props(invariants))
        proxy ! TargettedMessage(Server,
                    CraftedMessage(0, 0, OpInsert(0, "test.users", Stream(MongoDBObject("name" -> "Kali"))))
                )
        val transformed = expectMsgClass(classOf[TargettedMessage])
        transformed.direction should be(Server)
        val op:OpInsert = transformed.message.op.asInstanceOf[OpInsert]
        op.fullCollectionName should be("test.users")
        op.documents.size should be(1)
        op.documents.head should be(MongoDBObject("name" -> "Kali", "normName" -> "kali"))
    }

     override def afterAll { TestKit.shutdownActorSystem(system) }
}
