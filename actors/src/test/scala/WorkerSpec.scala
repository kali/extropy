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

case class RemoteControledContainer(label:String) extends Container {
    import mongo._
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject) {}
    def collection = label
    def iterator(payloadMongo:MongoClient) = {
        RemoteControledSyncRule.latch.set(1)
        while(RemoteControledSyncRule.latch.get() < 2)
            Thread.sleep(10)
        List()
    }
}

object RemoteControledSyncRule {
    def apply(junk:String) = Rule(CollectionContainer(junk), SameDocumentContact(), StringNormalizationProcessor("a", "b"))
    val latch = new java.util.concurrent.atomic.AtomicInteger(0)
}


class WorkerSpec extends TestKit(ActorSystem("workerspec"))
    with FlatSpecLike with ShouldMatchers with MongodbTemporary with Eventually {

    behavior of "An overseer"

    it should "manifest itelf as an agent" in withExtropy { (id,extropy) =>
        val name = "overseer-" + System.currentTimeMillis
        extropy.agentDAO.collection.size should be(0)
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        eventually { extropy.agentDAO.collection.size should be(1) }
    }

    it should "start a foreman to handle an invariant" in withExtropy { (id,extropy) =>
        val name = "overseer-" + id
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        val invariant = Invariant(RemoteControledSyncRule("foo"))
        extropy.invariantDAO.salat.save(invariant)
        extropy.invariantDAO.collection.size should be(1)
        eventually {
            Await.result(
                system.actorSelection(s"akka://workerspec/user/overseer-$id/foreman-${invariant._id}").resolveOne(10 millis),
                10 millis)
        }
    }

    behavior of "A foreman"

    it should "maintain its claim on an invariant" in withExtropy { (id,extropy) =>
        val invariant = Invariant(RemoteControledSyncRule("foo"))
        extropy.invariantDAO.salat.save(invariant)
        implicit val _locker = LockerIdentity(id.toString)
        val locked1 = extropy.prospect.get
        val foreman = system.actorOf(Foreman.props(extropy, locked1, _locker))
        extropy.invariantDAO.salat.findOneByID(locked1._id).get.emlp.until.getTime should not
                 be >(locked1.emlp.until.getTime + 500)
        eventually {
            extropy.invariantDAO.salat.findOneByID(locked1._id).get.emlp.until.getTime should
                 be >(locked1.emlp.until.getTime + 500)
        }
    }

    it should "switch its invariant from Created to Sync" taggedAs(Tag("r")) in withExtropy { (id,extropy) =>
        val invariant = Invariant(RemoteControledSyncRule("foo"))
        extropy.invariantDAO.salat.save(invariant)
        implicit val _locker = LockerIdentity(id.toString)
        val locked1 = extropy.prospect.get
        val foreman = system.actorOf(Foreman.props(extropy, locked1, _locker))
        extropy.agentDAO.readConfigurationVersion should be(0)
        eventually {
            extropy.invariantDAO.salat.findOneByID(locked1._id).get.status should be(InvariantStatus.Sync)
            extropy.invariantDAO.salat.findOneByID(locked1._id).get.statusChanging should be(true)
        }
        eventually {
            extropy.agentDAO.readConfigurationVersion should be(1)
        }
    }

    behavior of "A worker"

    it should "bring an invariant from Created to Run, through Sync" in withExtropy { (id,extropy) =>
        val name = "overseer-" + id
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        val otherAgent = system.actorOf(ExtropyAgent.props("agent-" + id, extropy, testActor))
        val invariant = Invariant(RemoteControledSyncRule("foo"))
        extropy.agentDAO.readConfigurationVersion should be(0)
        eventually {
            extropy.agentDAO.readMinimumConfigurationVersion should be(0)
        }
        extropy.invariantDAO.salat.save(invariant)
        var config = expectMsgClass(classOf[DynamicConfiguration])
        extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Sync)
        extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(true)
        Thread.sleep(3000)
        extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Sync)
        extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(true)
        otherAgent ! AckDynamicConfiguration(config)
        eventually {
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(false)
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Sync)
        }
        RemoteControledSyncRule.latch.get() should be(1)
        RemoteControledSyncRule.latch.set(2)
        eventually {
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(true)
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Run)
        }
        config = expectMsgClass(classOf[DynamicConfiguration])
        otherAgent ! AckDynamicConfiguration(config)
        eventually {
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(false)
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Run)
        }
    }

    // paraphernalia

    def withExtropy(testCode: (String, BaseExtropyContext) => Any) {
        val id = System.currentTimeMillis.toString
        val dbName = s"extropy-spec-$id"
        val extropy = ExtropyContext(mongoBackendClient(dbName), mongoBackendClient)
        // try {
            testCode(id, extropy)
        //}
    }

    override def afterAll {
        TestKit.shutdownActorSystem(system)
        super.afterAll
    }

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(50, Millis)))

}
