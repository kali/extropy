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

import mongo.MongoLockingPool.LockerIdentity

class WorkerSpec extends FlatSpec with ShouldMatchers with MongodbTemporary with Eventually with ExtropyFixture {

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(50, Millis)))


    behavior of "An overseer"

    it should "manifest itelf as an agent" in withExtropy { ctx:TextContext =>
        import ctx._
        val name = "overseer-" + System.currentTimeMillis
        extropy.agentDAO.collection.size should be(0)
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        eventually { extropy.agentDAO.collection.size should be(1) }
    }

    it should "start a foreman to handle an invariant" in withExtropy { ctx:TextContext =>
        import ctx._
        val name = "overseer-" + id
        val overseer = system.actorOf(Overseer.props(extropy, name), name)
        val invariant = StringNormalizationInvariant(new ObjectId(), "test.users", "name", "normName")
        extropy.invariantDAO.salat.save(invariant)
        extropy.invariantDAO.mlp.bless(invariant.id)
        extropy.invariantDAO.collection.size should be(1)
        eventually {
            Await.result(
                system.actorSelection(s"akka://$id/user/overseer-$id/foreman-${invariant.id}").resolveOne(10 millis),
                10 millis)
        }
    }

    behavior of "A foreman"

    it should "maintain its claim on an invariant" in withExtropy { ctx:TextContext =>
        import ctx._
        val invariant = StringNormalizationInvariant(new ObjectId(), "test.users", "name", "normName")
        extropy.invariantDAO.salat.save(invariant)
        extropy.invariantDAO.mlp.bless(invariant.id)
        implicit val _locker = LockerIdentity(id.toString)
        val locked1 = extropy.invariantDAO.prospect.get
        val foreman = system.actorOf(Foreman.props(extropy, locked1, _locker))
        Thread.sleep(2*extropy.foremanHeartBeat.toMillis)
    }

}

trait ExtropyFixture {

    def mongoBackendPort:Int

    case class TextContext(id:String, extropy:Extropy, system:ActorSystem)

    def withExtropy(testCode: TextContext => Any) {
        val id = System.currentTimeMillis.toString
        val dbName = s"db-$id"
        val system = ActorSystem(id)
        val extropy = Extropy(s"mongodb://localhost:$mongoBackendPort", dbName)
        try {
          testCode(TextContext(id, extropy, system))
        }
        finally {
            system.shutdown
            system.awaitTermination
            extropy.extropyMongoClient.dropDatabase(dbName)
            extropy.extropyMongoClient.close
        }
    }

}
