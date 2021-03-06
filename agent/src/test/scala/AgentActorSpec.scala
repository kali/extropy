/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }
import akka.testkit.{ TestKit, TestActor, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

class AgentSpec extends TestKit(ActorSystem("agentspec"))
        with FlatSpecLike with Matchers with BeforeAndAfterAll with ExtropyFixtures with Eventually {

    behavior of "An extropy agent"

    it should "prevent two agents to share their name" in withExtropyAndBlog { (extropy,blog) =>
        val agent1 = system.actorOf(ExtropyAgent.props(blog.dbName, extropy, testActor, None))
        an [Exception] should be thrownBy {
            system.actorOf(ExtropyAgent.props(blog.dbName, extropy, testActor, None))
        }
    }

    it should "maintain ping record in mongo" in withExtropyAndBlog { (extropy,blog) =>
        val agent = system.actorOf(ExtropyAgent.props(blog.dbName, extropy, testActor, None))
        eventually {
            extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> blog.dbName)) should not be('empty)
        }
        val agentDoc = extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> blog.dbName)).get
        agentDoc._id should be (blog.dbName)
        agentDoc.emlp.until.getTime should be > ( System.currentTimeMillis )
        agentDoc.emlp.until.getTime should be < ( System.currentTimeMillis + extropy.agentLockDuration.toMillis )
    }

    it should "notify configuration changes to its client" in withExtropyAndBlog { (extropy,blog) =>
        val agent = system.actorOf(ExtropyAgent.props(blog.dbName, extropy, testActor, None), "agent")
        eventually {
            extropy.agentDAO.bumpConfigurationVersion
            expectMsgClass(classOf[DynamicConfiguration])
        }
    }

    it should "propagate configuration changes ack to mongo" in withExtropyAndBlog { (extropy,blog) =>
        val agent = system.actorOf(ExtropyAgent.props(blog.dbName, extropy, testActor, None))
        agent ! AckDynamicConfiguration(DynamicConfiguration(12, List()))
        eventually {
            val agentDoc = extropy.agentDAO.salat.findOne(MongoDBObject("_id" -> blog.dbName)).get
            agentDoc.configurationVersion should be(12)
        }
        extropy.agentDAO.readMinimumConfigurationVersion should be(12L)
    }

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(12, Seconds)), interval = scaled(Span(500, Millis)))

    override def afterAll { TestKit.shutdownActorSystem(system) ; super.afterAll }
}
