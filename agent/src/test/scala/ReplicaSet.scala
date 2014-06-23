/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import java.net.InetSocketAddress

import scala.concurrent.duration._
import scala.language.postfixOps

import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, PoisonPill }
import akka.testkit.{ TestKit, TestActor }

import de.flapdoodle.embed.mongo.{ MongodExecutable, MongodProcess, MongodStarter, Command }
import de.flapdoodle.embed.process.runtime._
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.{ IFeatureAwareVersion, Version }

import com.mongodb.casbah.Imports._
import org.bson.BSONObject
import mongoutils.BSONObjectConversions._

import scala.collection.mutable.Buffer

object SlowTest extends Tag("slow")

class ReplicaSet {

    def mongoWantedVersion:Option[IFeatureAwareVersion] = None

    val ports:Buffer[Int] = Buffer.fill(3)(-1)
    var mongoExecutables:Array[MongodExecutable] = Array.fill(3)(null)
    var mongoProcesses:Array[MongodProcess] = Array.fill(3)(null)

    var mongoBackendClient:MongoClient = null

    val runtime:MongodStarter = {
        val runtimeConfig = new RuntimeConfigBuilder()
                                .defaults(Command.MongoD)
                                .processOutput(ProcessOutput.getDefaultInstanceSilent())
                                .build
        MongodStarter.getInstance(runtimeConfig)
    }

    def start {
        var members:Array[MongoDBObject] = Array.fill(3)(null)
        (0 until 3).foreach { i =>
            ports(i) = Network.getFreeServerPort
            val config = new MongodConfigBuilder()
                            .net(new Net(ports(i), false))
                            .version(mongoWantedVersion.getOrElse(Version.Main.PRODUCTION))
                            .replication(new Storage(null, "rs", 0))
                            .build
            mongoExecutables(i) = runtime.prepare(config)
            mongoProcesses(i) = mongoExecutables(i).start
            members(i) = MongoDBObject("_id" -> i, "host" ->
                (config.net.getServerAddress.getHostName() + ":" + config.net.getPort))
        }
        mongoBackendClient = MongoClient("127.0.0.1", ports(0))
        mongoBackendClient("admin").command(
            MongoDBObject("replSetInitiate" -> MongoDBObject("_id" -> "rs", "members" -> members))
        )
        Thread.sleep(1000)
    }

    def up = {
        val s = status
        s.as[List[BSONObject]]("members").count( _.as[String]("stateStr") == "PRIMARY") == 1 &&
        s.as[List[BSONObject]]("members").count( _.as[String]("stateStr") == "SECONDARY") == 2
    }

    def waitForUp(limit:Duration) {
        val interval = 1 seconds
        val started = System.currentTimeMillis
        while(!up && (System.currentTimeMillis - started) < limit.toMillis) {
            Thread.sleep(interval.toMillis)
        }
    }

    def stop {
        mongoProcesses.filter( _ != null).foreach( _.stop )
        mongoExecutables.filter( _ != null).foreach( _.stop )
    }

    def status = mongoBackendClient("admin").command("replSetGetStatus")
}

class ReplicaSetSupportSpec extends TestKit(ActorSystem("replicaset"))
        with FlatSpecLike with Eventually with Matchers with BeforeAndAfterAll {

    val replicaset = new ReplicaSet
    val proxiesPorts:Buffer[Int] = Buffer.fill(3)(-1)
    val proxies:Buffer[ActorRef] = Buffer.fill(3)(null)

    override def beforeAll { replicaset.start ; replicaset.waitForUp(1 minute) ; startProxies }
    override def afterAll { replicaset.stop }

    behavior of "Replica set testing prerequisites"

    it should "start a working replica set" taggedAs(SlowTest) in {
        val conf = replicaset.mongoBackendClient("local")("system.replset").findOne().get
        conf.as[List[_]]("members") should have size(3)
        val status = replicaset.status
        status.as[List[BSONObject]]("members").count( _.as[String]("stateStr") == "PRIMARY") should be(1)
        status.as[List[BSONObject]]("members").count( _.as[String]("stateStr") == "SECONDARY") should be(2)
    }

    it should "start a proxy for each member" taggedAs(SlowTest) in {
    }

    def startProxies {
        val now = System.currentTimeMillis
        val payloadDbName = s"extropy-spec-payload-$now"
        val extropyDbName = s"extropy-spec-internal-$now"
        val fixture = BlogFixtures(payloadDbName)
        Thread.sleep(1000)

        replicaset.status.as[List[BSONObject]]("members").zipWithIndex.foreach { case (obj,index) =>
            val backendLocation = obj.as[String]("name")
            val backendPort = backendLocation.split(':').last.toInt
            val extropy = ExtropyContext(replicaset.mongoBackendClient(extropyDbName), replicaset.mongoBackendClient)
            proxiesPorts(index) = de.flapdoodle.embed.process.runtime.Network.getFreeServerPort
            proxies(index) = system.actorOf(ProxyServer.props(
                extropy,
                new InetSocketAddress("127.0.0.1", proxiesPorts(index)),
                new InetSocketAddress("127.0.0.1", backendPort)
            ))
        }
        eventually {
            replicaset.mongoBackendClient(extropyDbName)("agents") should have size(3)
        }
    }

    behavior of "Extropy replica set support"

    it should "detect it is running against a replica set setup" taggedAs(SlowTest) in {
        val extropy = ExtropyContext(replicaset.mongoBackendClient("whatever"), replicaset.mongoBackendClient)
        extropy.clusterKind should be(ReplicaKind)
    }

    it should "register proxy mappings in agent collection" taggedAs(SlowTest) in {
    }

    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(2000, Millis)))
}
