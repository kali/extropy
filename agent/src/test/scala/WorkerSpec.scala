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

object RemoteControlLatch {
    val latch = new java.util.concurrent.atomic.AtomicInteger(0)
}

case class RemoteControledContainer(name:String) extends Container {
    val inner = CollectionContainer(name)
    def collection = inner.collection
    def collectionName = inner.collectionName
    def dbName = inner.dbName
    def pull(payloadMongo:MongoClient,loc:Location):Iterable[DBObject] = inner.pull(payloadMongo, loc)
    def setValues(payloadMongo:MongoClient,location:Location,values:MongoDBObject) {
        inner.setValues(payloadMongo, location, values)
    }
    def iterator(payloadMongo:MongoClient) = {
        RemoteControlLatch.latch.set(1)
        while(RemoteControlLatch.latch.get() < 2)
            Thread.sleep(10)
        inner.iterator(payloadMongo)
    }
    def toLabel = "RemoteControledContainer"
}

object RemoteControledSyncRule {
    def apply(label:String) =
        Rule(new RemoteControledContainer(label), new RemoteControledContainer(label), SameDocumentTie(), StringNormalizationReaction("foo", "bar"))
}

class WorkerSpec extends TestKit(ActorSystem("workerspec"))
    with FlatSpecLike with ShouldMatchers with ExtropyFixtures with Eventually {

    behavior of "An overseer"

    it should "manifest itelf as an agent" in withExtropyAndBlog { (extropy,blog) =>
        val name = "overseer-" + System.currentTimeMillis
        extropy.agentDAO.collection.size should be(0)
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        eventually { extropy.agentDAO.collection.size should be(1) }
    }

    it should "start foremen to handle all invariants" in withExtropyAndBlog { (extropy,blog) =>
        val name = "overseer-" + blog.dbName
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        extropy.invariantDAO.collection.size should be >(0)
        eventually {
            extropy.invariantDAO.salat.find(MongoDBObject.empty).foreach { invariant =>
                Await.result(
                    system.actorSelection(s"akka://workerspec/user/$name/foreman-${invariant._id}").resolveOne(10 millis),
                    10 millis)
            }
        }
    }

    behavior of "A foreman"

    it should "maintain its claim on an invariant" in withExtropyAndBlog { (extropy,blog) =>
        val invariant = Invariant(RemoteControledSyncRule("foo.bar"))
        extropy.invariantDAO.salat.save(invariant)
        implicit val _locker = LockerIdentity(blog.dbName)
        val locked1 = extropy.prospect.get
        val foreman = system.actorOf(Foreman.props(extropy, locked1, _locker))
        extropy.invariantDAO.salat.findOneByID(locked1._id).get.emlp.until.getTime should not
                 be >(locked1.emlp.until.getTime + 500)
        eventually {
            extropy.invariantDAO.salat.findOneByID(locked1._id).get.emlp.until.getTime should
                 be >(locked1.emlp.until.getTime + 500)
        }
    }

    it should "switch its invariant from Created to Sync" in withExtropyAndBlog { (extropy,blog) =>
        val invariant = Invariant(RemoteControledSyncRule("foo.baz"))
        extropy.invariantDAO.salat.save(invariant)
        implicit val _locker = LockerIdentity(blog.dbName)
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

    it should "bring an invariant from Created to Run, through Sync" in withExtropyAndBlog { (extropy,blog) =>
        // don't actually use the blog fixture
        extropy.invariantDAO.salat.remove(MongoDBObject.empty)
        extropy.agentDAO.readConfigurationVersion should be(0)

        val invariant = Invariant(RemoteControledSyncRule("foo.bar"))

        val name = "overseer"
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        val otherAgent = system.actorOf(ExtropyAgent.props("other", extropy, testActor), "other")
        eventually {
            extropy.agentDAO.readMinimumConfigurationVersion should be(0)
        }
        extropy.invariantDAO.salat.save(invariant)
        extropy.agentDAO.bumpConfigurationVersion

        eventually {
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Sync)
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(true)
        }
        Thread.sleep(3000)
        extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Sync)
        extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(true)

        eventually {
            val config2 = expectMsgClass(classOf[DynamicConfiguration])
            config2.version should be(2L)
            otherAgent ! AckDynamicConfiguration(config2)
        }

        eventually {
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(true)
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Sync)
        }
        eventually {
            RemoteControlLatch.latch.get() should be(1)
        }
        RemoteControlLatch.latch.set(2)
        eventually {
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(true)
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Run)
        }

        val config3 = expectMsgClass(classOf[DynamicConfiguration])
        config3.version should be(3L)
        otherAgent ! AckDynamicConfiguration(config3)

        eventually {
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.statusChanging should be(false)
            extropy.invariantDAO.salat.findOneByID(invariant._id).get.status should be(InvariantStatus.Run)
        }
    }

    // paraphernalia

    override def afterAll {
        TestKit.shutdownActorSystem(system)
        super.afterAll
    }

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(250, Millis)))

}
