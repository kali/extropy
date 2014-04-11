package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }
import akka.testkit.{ TestKit, TestActor, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

object KaliTest extends Tag("KaliTest")

class MyActor extends Actor { def receive = Actor.emptyBehavior }

class AgentSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll
        with MongodbTemporary with Eventually with ExtropyFixture {

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(50, Millis)))

    behavior of "An extropy agent"


    it should "prevent two agents to share their name" in withExtropy { ctxt =>
        import ctxt._
        implicit val system = ctxt.system
        val act = system.actorOf(Props[MyActor])
        val agent1 = system.actorOf(ExtropyAgent.props(id, extropy, act))
        an [Exception] should be thrownBy {
            system.actorOf(ExtropyAgent.props(id, extropy, act))
        }
    }

    it should "maintain ping record in mongo" in withExtropy { ctxt =>
        import ctxt._
        val act = system.actorOf(Props[MyActor])
        val agent = system.actorOf(ExtropyAgent.props(id, extropy, act))
        eventually {
            extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> id)) should not be('empty)
        }
        val agentDoc = extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> id)).get
        agentDoc._id should be (id)
        agentDoc.emlp.until.getTime should be > ( System.currentTimeMillis )
        agentDoc.emlp.until.getTime should be < ( System.currentTimeMillis + extropy.agentLockDuration.toMillis )
    }

    it should "propagate configuration changes ack to mongo" in withExtropy { ctxt =>
        import ctxt._
        val act = system.actorOf(Props[MyActor])
        val agent = system.actorOf(ExtropyAgent.props(id, extropy, act))
        agent ! AckDynamicConfiguration(DynamicConfiguration(12, List()))
        eventually {
            val agentDoc = extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> id)).get
            agentDoc.configurationVersion should be(12)
        }
    }

    it should "notify configuration changes to its client" in withExtropy { ctxt =>
        pending
        import ctxt._
        val act = system.actorOf(Props[MyActor])
        val agent = system.actorOf(ExtropyAgent.props(id, extropy, act), "agent")
        Thread.sleep(250)
        val conf = extropy.agentDAO.bumpConfigurationVersion
/*
        expectMsgClass(act)(classOf[DynamicConfiguration])
*/
    }


}
