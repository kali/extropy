package org.zoy.kali.extropy

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.time._
import org.scalatest.concurrent.Eventually

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill, ActorPath }
import akka.testkit.{ TestKit, TestActorRef, ImplicitSender, CallingThreadDispatcher }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._
import scala.concurrent.Await

import mongoutils._

class WorkerSpec extends TestKit(ActorSystem("workerspec"))
    with FlatSpecLike with ShouldMatchers with MongodbTemporary with Eventually {

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(50, Millis)))

    behavior of "An overseer"

    def withExtropy(testCode: (String, BaseExtropyContext) => Any) {
        val id = System.currentTimeMillis.toString
        val dbName = s"extropy-spec-$id"
        val extropy = Extropy(mongoBackendClient(dbName))
        // try {
            testCode(id, extropy)
        //}
    }

    it should "manifest itelf as an agent" in withExtropy { (id,extropy) =>
        val name = "overseer-" + System.currentTimeMillis
        extropy.agentDAO.collection.size should be(0)
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        eventually { extropy.agentDAO.collection.size should be(1) }
    }

    it should "start a foreman to handle an invariant" in withExtropy { (id,extropy) =>
        val name = "overseer-" + id
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        val invariant = Invariant(StringNormalizationRule("test.users", "name", "normName"))
        extropy.invariantDAO.salat.save(invariant)
        extropy.invariantDAO.mlp.bless(invariant._id)
        extropy.invariantDAO.collection.size should be(1)
        eventually {
            Await.result(
                system.actorSelection(s"akka://workerspec/user/overseer-$id/foreman-${invariant._id}").resolveOne(10 millis),
                10 millis)
        }
    }

    behavior of "A foreman"

    it should "maintain its claim on an invariant" taggedAs(Tag("r")) in withExtropy { (id,extropy) =>
        val invariant = Invariant(StringNormalizationRule("test.users", "name", "normName"))
        extropy.invariantDAO.salat.save(invariant)
        extropy.invariantDAO.mlp.bless(invariant._id)
        implicit val _locker = LockerIdentity(id.toString)
        val locked1 = extropy.invariantDAO.prospect.get
        val foreman = system.actorOf(Foreman.props(extropy, locked1, _locker))
        extropy.invariantDAO.salat.findOneByID(locked1._id).get.emlp.until.getTime should not
                 be >(locked1.emlp.until.getTime + 500)
        eventually {
            extropy.invariantDAO.salat.findOneByID(locked1._id).get.emlp.until.getTime should
                 be >(locked1.emlp.until.getTime + 500)
        }
    }

    override def afterAll {
        TestKit.shutdownActorSystem(system)
        super.afterAll
    }

}
