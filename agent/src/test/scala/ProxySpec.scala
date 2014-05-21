package org.zoy.kali.extropy

import java.net.InetSocketAddress

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import com.novus.salat._
import custom.ctx

import scala.concurrent.duration._

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }

import com.mongodb.casbah.Imports._
import akka.testkit.{ TestKit, TestActor }

import de.flapdoodle.embed.mongo.distribution.{ IFeatureAwareVersion, Version }

abstract class FullStackSpec extends TestKit(ActorSystem("proxyspec")) with FlatSpecLike
        with ExtropyFixtures with Matchers with Eventually {

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(3, Seconds)), interval = scaled(Span(100, Millis)))

    def withProxiedClient(testCode:(BaseExtropyContext, BlogFixtures, MongoClient)=>Any, loadRules:Boolean=true) {
        withExtropyAndBlog({ (extropy,blog) =>
            val port = de.flapdoodle.embed.process.runtime.Network.getFreeServerPort
            val proxy = system.actorOf(ProxyServer.props(
                extropy,
                new InetSocketAddress("127.0.0.1", port),
                new InetSocketAddress("127.0.0.1", mongoBackendPort)
            ), "proxy" + blog.dbName)

            (1 to 30).find { i =>
                try {
                    Thread.sleep(100)
                    new java.net.Socket("127.0.0.1", port)
                    true
                } catch { case e:Throwable => false }
            }

            val mongoClient = MongoClient("127.0.0.1", port)
            try {
                testCode(extropy, blog, mongoClient)
            } finally {
                proxy ! PoisonPill
            }
        }, loadRules)
    }

    override def afterAll { TestKit.shutdownActorSystem(system) ; super.afterAll }
}

class FullStackProxySpec extends FullStackSpec {

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(500, Millis)))

    behavior of s"An extropy proxy"

    it should "propagate and acknowledge configuration bumps" in withProxiedClient { (extropy, blog, mongoClient) =>
        val db = mongoClient("test")
        val initial = extropy.agentDAO.bumpConfigurationVersion
        eventually {
            db("$extropy").findOne(MongoDBObject("configVersion" -> 1)) should
                be(Some(MongoDBObject("ok" -> 1, "version" -> initial )))
        }
        val next = extropy.agentDAO.bumpConfigurationVersion
        next should be >(initial)
        eventually {
            db("$extropy").findOne(MongoDBObject("configVersion" -> 1)) should
                be(Some(MongoDBObject("ok" -> 1, "version" -> next )))
        }

        eventually { extropy.agentDAO.collection.size should be(1) }
        extropy.agentDAO.collection.findOne(MongoDBObject.empty).get.getAs[Long]("configurationVersion").get should be(next)
    }

    it should "handle addInvariant command" in withProxiedClient({ (extropy, blog, mongoClient) =>
        import blog._
        val db = mongoClient(dbName)
        allRules.size should be > (0)
        allRules.foreach { rule =>
            db("$extropy").findOne(MongoDBObject( "addInvariant" -> { grater[Invariant].asDBObject(Invariant(rule)) } )) should be(
                Some(MongoDBObject("ok" -> 1))
            )
        }
        extropy.invariantDAO.collection.size should be( allRules.size )
    }, false)

    it should "handle addRule command" in withProxiedClient({ (extropy, blog, mongoClient) =>
        import blog._
        val db = mongoClient(dbName)
        allRules.size should be > (0)
        allRules.foreach { rule =>
             db("$extropy").findOne(MongoDBObject( "addRule" -> rule.toMongo )) should be(Some(MongoDBObject("ok" -> 1)))
        }
        extropy.invariantDAO.collection.size should be( allRules.size )
    }, false)

    behavior of "a proxy + worker"

    it should "should get new invariants in verified state" in withProxiedClient({ (extropy, blog, mongoClient) =>
        import blog._
        val overseer = system.actorOf(Overseer.props(extropy, dbName), dbName)
        extropy.invariantDAO.collection.size should be( 0 )
        val db = mongoClient(dbName)
        db("posts").insert(post1, post2)
        db("users").insert(userLiz, userJack)
        allRules.size should be > (0)
        allRules.foreach( _.checkAll(mongoClient) should not be ( 'empty ) )
        allRules.foreach { rule =>
             db("$extropy").findOne(MongoDBObject( "addInvariant" -> { grater[Invariant].asDBObject(Invariant(rule)) } ))
        }
        extropy.invariantDAO.collection.size should be( allRules.size )
        eventually {
            allRules.foreach( _.checkAll(mongoClient) should be ( 'empty ))
        }
    }, false)
}


abstract class BaseProtoServerSpec extends FullStackSpec {

    behavior of s"An extropy proxy with protocol: $mongoWantedVersion"

    it should "deal with insert" in withProxiedClient { (extropy, blog, client) =>
        import blog._
        client(dbName)("posts").insert(post1)
        client(dbName)("posts").size should be(1)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
    }

    it should "deal with update" in withProxiedClient { (extropy, blog, client) =>
        import blog._
        client(dbName)("users").insert(userLiz)
        client(dbName)("posts").insert(post1)
        client(dbName)("posts").size should be(1)
        client(dbName)("users").size should be(1)
        client(dbName)("users").update(MongoDBObject("_id" -> "liz"), MongoDBObject("$set" -> MongoDBObject("name" -> "Foobar")))
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
        client(dbName)("users").update(MongoDBObject("_id" -> "jack"), MongoDBObject("$set" -> MongoDBObject("name" -> "Foobar")))
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
    }

    it should "deal with delete" in withProxiedClient { (extropy, blog, client) =>
        import blog._
        client(dbName)("users").insert(userLiz)
        client(dbName)("posts").insert(post1)
        client(dbName)("posts").size should be(1)
        client(dbName)("users").size should be(1)
        client(dbName)("posts").remove(MongoDBObject("_id" -> "post1"))
        client(dbName)("posts").size should be(0)
        allRules.foreach { rule => rule.checkAll(extropy.payloadMongo) should be ('empty) }
    }


    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(3, Seconds)), interval = scaled(Span(100, Millis)))

}

class Proto2_4 extends BaseProtoServerSpec {
    override def mongoWantedVersion = Some(Version.Main.V2_4)
}

class Proto2_6 extends BaseProtoServerSpec {
    override def mongoWantedVersion = Some(Version.Main.V2_6)
}
