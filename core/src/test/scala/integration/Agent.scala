package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

object KaliTest extends Tag("KaliTest")

class AgentSpec extends TestKit(ActorSystem()) with ImplicitSender
        with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll
        with MongodbTemporary {

    behavior of "An extropy agent"

    var context:Extropy = null
    override def beforeAll {
        super.beforeAll
        context = Extropy(s"mongodb://localhost:$mongoBackendPort")
    }

    it should "prevent two agents to share their name" in {
        val id = "agent-1"
        val agent1 = system.actorOf(ExtropyAgent.props(id, context, self))
        an [Exception] should be thrownBy {
            system.actorOf(ExtropyAgent.props(id, context, self))
        }
        agent1 ! PoisonPill
    }

    it should "maintain ping record in mongo" in {
        val id = "agent-2"
        val agent = system.actorOf(ExtropyAgent.props(id, context, self))
        Thread.sleep(context.pingHeartBeat.toMillis*2)
        val agentDoc = context.agentDAO.salat.findOne(MongoDBObject("_id" -> id)).get
        agentDoc._id should be (id)
        agentDoc.emlp.until.getTime should be > ( System.currentTimeMillis )
        agentDoc.emlp.until.getTime should be < ( System.currentTimeMillis + context.pingValidity.toMillis )
        agent ! PoisonPill
    }

    it should "notify configuration changes to its client" in {
        val id = "agent-3"
        val agent = system.actorOf(ExtropyAgent.props(id, context, self), "agent")
        Thread.sleep(250)
        val conf = context.agentDAO.bumpConfigurationVersion
        expectMsgClass(classOf[DynamicConfiguration])
        agent ! PoisonPill
    }

    it should "propagate configuration changes ack to mongo" in {
        val id = "agent-" + System.currentTimeMillis
        context.agentDAO.salat.remove(MongoDBObject.empty)
        val agent = system.actorOf(ExtropyAgent.props(id, context, self))
        agent ! AckDynamicConfiguration(DynamicConfiguration(12, List()))
        Thread.sleep(500)
        val agentDoc = context.agentDAO.salat.findOne(MongoDBObject("_id" -> id)).get
        agentDoc.configurationVersion should be(12)
        agent ! PoisonPill
    }

    override def afterAll { super.afterAll; TestKit.shutdownActorSystem(system) }
}
