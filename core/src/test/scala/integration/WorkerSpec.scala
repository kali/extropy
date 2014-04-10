package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill, ActorPath }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._
import scala.concurrent.Await

class WorkerSpec extends TestKit(ActorSystem()) with ImplicitSender
        with FlatSpecLike with ShouldMatchers with BeforeAndAfterAll
        with MongodbTemporary {

    behavior of "An overseer"

    it should "manifest itelf as an agent" in {
        val id = System.currentTimeMillis
        val extropy = Extropy(s"mongodb://localhost:$mongoBackendPort", "db"+id)
        val name = "overseer-" + System.currentTimeMillis
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        Thread.sleep(100)
        extropy.agentDAO.collection.size should be(1)
        overseer ! PoisonPill
        Thread.sleep(500)
        extropy.extropyMongoClient.close
    }

    it should "start a foreman to handle an invariant" taggedAs(KaliTest) in {
        val id = System.currentTimeMillis
        val extropy = Extropy(s"mongodb://localhost:$mongoBackendPort", "db"+id)
        val name = "overseer-" + id
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        Thread.sleep(500)
        val invariant = StringNormalizationInvariant(new ObjectId(), "test.users", "name", "normName")
        extropy.invariantDAO.salat.save(invariant)
        extropy.invariantDAO.mlp.bless(invariant.id)
        Thread.sleep(500)
        Await.result(
            system.actorSelection(s"akka://default/user/overseer-$id/foreman-${invariant.id}").resolveOne(1 second),
            1 second)
        overseer ! PoisonPill
        Thread.sleep(500)
        extropy.extropyMongoClient.close
    }

    override def beforeAll {
        super.beforeAll
    }
    override def afterAll { super.afterAll; TestKit.shutdownActorSystem(system) }
}
