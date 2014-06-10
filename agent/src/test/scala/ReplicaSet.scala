/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import org.scalatest._

import de.flapdoodle.embed.mongo.{ MongodExecutable, MongodProcess, MongodStarter, Command }
import de.flapdoodle.embed.process.runtime._
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.mongo.config._
import de.flapdoodle.embed.mongo.distribution.{ IFeatureAwareVersion, Version }

import com.mongodb.casbah.Imports._

class ReplicaSet {

    def mongoWantedVersion:Option[IFeatureAwareVersion] = None

    var ports:Array[Int] = Array.fill(3)(-1)
    var mongoExecutables:Array[MongodExecutable] = Array.fill(3)(null)
    var mongoProcesses:Array[MongodProcess] = Array.fill(3)(null)

    var mongoBackendClient:MongoClient = null

    val runtime:MongodStarter = {
        val runtimeConfig = new RuntimeConfigBuilder()
                                .defaults(Command.MongoD)
 //                               .processOutput(ProcessOutput.getDefaultInstanceSilent())
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

    def stop {
        mongoProcesses.filter( _ != null).foreach( _.stop )
        mongoExecutables.filter( _ != null).foreach( _.stop )
    }
}

class SomeSpec extends FlatSpecLike {
    behavior of "Replica Set"

    it should "start a working replica set" taggedAs(Tag("k")) in {
        val replicaset = new ReplicaSet
        replicaset.start
        val conf = replicaset.mongoBackendClient("local")("system.replset").findOne().get
        replicaset.stop
    }
}
