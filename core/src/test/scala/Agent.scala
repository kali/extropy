package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }
import akka.testkit.{ TestKit, TestActor, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

class AgentSpec extends TestKit(ActorSystem("agentspec"))
        with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll with MongodbTemporary with Eventually {

    behavior of "An extropy agent"

    it should "prevent two agents to share their name" in withExtropy { (id,extropy) =>
        val agent1 = system.actorOf(ExtropyAgent.props(id, extropy, testActor))
        an [Exception] should be thrownBy {
            system.actorOf(ExtropyAgent.props(id, extropy, testActor))
        }
    }

    it should "maintain ping record in mongo" in withExtropy { (id,extropy) =>
        val agent = system.actorOf(ExtropyAgent.props(id, extropy, testActor))
        eventually {
            extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> id)) should not be('empty)
        }
        val agentDoc = extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> id)).get
        agentDoc._id should be (id)
        agentDoc.emlp.until.getTime should be > ( System.currentTimeMillis )
        agentDoc.emlp.until.getTime should be < ( System.currentTimeMillis + extropy.agentLockDuration.toMillis )
    }

    it should "notify configuration changes to its client" in withExtropy { (id,extropy) =>
        val agent = system.actorOf(ExtropyAgent.props(id, extropy, testActor), "agent")
        eventually {
            extropy.agentDAO.bumpConfigurationVersion
            expectMsgClass(classOf[DynamicConfiguration])
        }
    }

    it should "propagate configuration changes ack to mongo" in withExtropy { (id,extropy) =>
        val agent = system.actorOf(ExtropyAgent.props(id, extropy, testActor))
        agent ! AckDynamicConfiguration(DynamicConfiguration(12, List()))
        eventually {
            val agentDoc = extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> id)).get
            agentDoc.configurationVersion should be(12)
        }
    }

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(50, Millis)))

    def withExtropy(testCode: (String, BaseExtropyContext) => Any) {
        val id = System.currentTimeMillis.toString
        val dbName = s"extropy-spec-$id"
        val extropy = Extropy(mongoBackendClient(dbName))
        try {
            testCode(id, extropy)
        } finally {
        }
    }

    override def afterAll { TestKit.shutdownActorSystem(system) ; super.afterAll }

}
