package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
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

    it should "maintain ping record in mongo" in {
        val id = "agent-" + System.currentTimeMillis
        val agent = system.actorOf(ExtropyAgent.props(id, context, self))
        Thread.sleep(250)
        val agents = context.agentDAO.find(MongoDBObject()).toList
        agents.size should be(1)
        agents.head._id should be (id)
        agents.head.until.getTime should be > ( System.currentTimeMillis )
        agents.head.until.getTime should be < ( System.currentTimeMillis + 2500 )
        system.stop(agent)
        Thread.sleep(50)
        context.agentDAO.count() should be(0)
    }

    it should "notify configuration changes to its client" taggedAs(KaliTest) in {
        val id = "agent-" + System.currentTimeMillis
        context.agentDAO.remove(MongoDBObject.empty)
        val agent = system.actorOf(ExtropyAgent.props(id, context, self), "agent")
        Thread.sleep(250)
        val conf = context.agentDAO.bumpConfigurationVersion
        expectMsgClass(classOf[DynamicConfiguration])
        system.stop(agent)
    }

    it should "propagate configuration changes ack to mongo" in {
        val id = "agent-" + System.currentTimeMillis
        context.agentDAO.remove(MongoDBObject.empty)
        val agent = system.actorOf(ExtropyAgent.props(id, context, self))
        agent ! AckDynamicConfiguration(DynamicConfiguration(12, List()))
        Thread.sleep(500)
        val agentDoc = context.agentDAO.findOne(MongoDBObject()).get
        agentDoc.configurationVersion should be(12)
        system.stop(agent)
    }

    override def afterAll { TestKit.shutdownActorSystem(system) }
}
