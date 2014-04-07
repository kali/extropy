package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

class AgentSpec extends TestKit(ActorSystem()) with ImplicitSender
        with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll with MongodbTemporary {

    behavior of "An extropy agent"

    it should "maintain ping record in mongo" in {
        val id = "agent-" + System.currentTimeMillis
        val context = new BaseExtropyContext {
            def extropyMongoUrl = "mongodb://localhost:" + mongoBackendPort
        }
        context.agentDAO.count() should be(0)
        val agent = system.actorOf(ExtropyAgent.props(id, context.agentDAO, context.invariantDAO))
        Thread.sleep(50)
        context.agentDAO.count() should be(1)
        val agents = context.agentDAO.find(MongoDBObject()).toList
        agents.size should be(1)
        agents.head._id should be (id)
        agents.head.until.getTime should be > ( System.currentTimeMillis )
        agents.head.until.getTime should be < ( System.currentTimeMillis + 2500 )
        system.stop(agent)
        Thread.sleep(50)
        context.agentDAO.count() should be(0)
    }

    override def afterAll { TestKit.shutdownActorSystem(system) }
}
