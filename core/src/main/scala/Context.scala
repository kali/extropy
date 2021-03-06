/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import scala.concurrent.duration._

import com.mongodb.casbah.Imports._

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._

import scala.concurrent.duration._

import mongoutils._

import BSONObjectConversions._

package object custom {
  implicit val ctx = new Context() {
    val name = "custom_transformer_spec"
    override val typeHintStrategy = StringTypeHintStrategy(TypeHintFrequency.WhenNecessary, "_t")
    registerCustomTransformer(RuleCodec)
  }
}

import custom.ctx

abstract sealed class ClusterKind;
object ShardedKind extends ClusterKind;
object ReplicaKind extends ClusterKind;
object StandaloneKind extends ClusterKind;


trait BaseExtropyContext {
    val hostname = java.net.InetAddress.getLocalHost.getHostName

    def agentHeartBeat = 1.second
    def agentLockDuration = 1.minute

    def overseerHeartBeat = 1.second
    def foremanHeartBeat = 1.second
    def invariantLockDuration = foremanHeartBeat * 10

    def agentDAO:ExtropyAgentDescriptionDAO
    def invariantDAO:InvariantDAO

    val payloadMongo:MongoClient

    // invariant stuff
    def prospect(implicit by:LockerIdentity):Option[Invariant] =
        invariantDAO.mlp.lockOne().map( invariantDAO.salat._grater.asObject(_) )
    def claim(invariant:Invariant)(implicit by:LockerIdentity):Invariant =
        invariantDAO.salat._grater.asObject(invariantDAO.mlp.relock(invariantDAO.salat._grater.asDBObject(invariant)))

    def switchInvariantTo(invariant:Invariant, status:InvariantStatus.Value):Long = {
        invariantDAO.collection.update(MongoDBObject("_id" -> invariant._id),
            MongoDBObject("$set" -> MongoDBObject("status" -> status.toString, "statusChanging" -> true)))
        agentDAO.bumpConfigurationVersion
    }

    def ackStatusChange(invariant:Invariant) {
        invariantDAO.collection.update(MongoDBObject("_id" -> invariant._id),
            MongoDBObject("$set" -> MongoDBObject("statusChanging" -> false)))
    }
    def ackCommand(invariant:Invariant) {
        invariantDAO.collection.update(MongoDBObject("_id" -> invariant._id),
            MongoDBObject("$unset" -> MongoDBObject("command" -> true)))
    }

    // dynamic configuration management
    def pullConfiguration = DynamicConfiguration(agentDAO.readConfigurationVersion,
        invariantDAO.salat.find(MongoDBObject.empty).toList)

    // replica set mapping management
    val clusterKind:ClusterKind = {
        if(payloadMongo("config")("version").count() > 0)
            ShardedKind
        else if(payloadMongo("local")("oplog.rs").count() > 0 || payloadMongo("local")("oplog.rs").count() > 0)
            ReplicaKind
        else
            StandaloneKind
    }

}

case class ExtropyContext(val extropyDatabase:MongoDB, payloadMongo:MongoClient) extends BaseExtropyContext {
    val agentDAO = new ExtropyAgentDescriptionDAO(extropyDatabase, agentLockDuration)
    val invariantDAO = new InvariantDAO(extropyDatabase, invariantLockDuration)
}

case class DynamicConfiguration(version:Long, invariants:List[Invariant])
object DynamicConfiguration {
    val empty = DynamicConfiguration(-1, List())
}
case class AckDynamicConfiguration(config:DynamicConfiguration)

