/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import java.util.Date
import com.mongodb.casbah.Imports._

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

import mongoutils._

import com.typesafe.scalalogging.slf4j.StrictLogging

case class ProxyMapping(proxy:String, backend:String)
case class ExtropyAgentDescription(_id:String, emlp:MongoLock, configurationVersion:Long=(-1L), mapping:Option[ProxyMapping])

class ExtropyAgentDescriptionDAO(val db:MongoDB, val pingValidity:FiniteDuration)(implicit ctx:Context)
        extends StrictLogging {
    val collection = db("agents")
    val salat = new SalatDAO[ExtropyAgentDescription,ObjectId](collection) {}

    val agentMLP = MongoLockingPool(collection, pingValidity)

    def register(agent:ExtropyAgentDescription) {
        agentMLP.cleanupOldLocks
        logger.debug(s"Registering agent:$agent")
        agentMLP.insertLocked(grater[ExtropyAgentDescription].asDBObject(agent))(LockerIdentity(agent._id))
    }

    def ping(id:String, validity:FiniteDuration) {
        logger.trace(s"Ping agent:$id")
        agentMLP.relock(MongoDBObject("_id" -> id), validity)(LockerIdentity(id))
    }

    def unregister(id:String) {
        logger.debug(s"Unregister agent:$id")
        agentMLP.release(MongoDBObject("_id" -> id), delete=true)(LockerIdentity(id))
    }

    def ackVersion(id:String, version:Long) {
        logger.debug(s"Agent ack version: agent:$id version:$version")
        collection.update(MongoDBObject("_id" -> id),
            MongoDBObject("$set" -> MongoDBObject("configurationVersion" -> version)))
    }

    val versionCollection = db("configuration_version")
    def readConfigurationVersion:Long = versionCollection.findOne(MongoDBObject("_id" -> "version"))
                        .flatMap( d => d.getAs[Long]("value") ).getOrElse(0L)
    def bumpConfigurationVersion:Long = versionCollection.findAndModify(
            query=MongoDBObject("_id" -> "version"),
            update=MongoDBObject("$inc" -> MongoDBObject("value" -> 1L)),
            sort=null, fields=null, upsert=true, remove=false, returnNew=true).flatMap( _.getAs[Long]("value") ).get
    def readMinimumConfigurationVersion:Long = {
        val all = collection.distinct("configurationVersion").map( _.toString.toLong )
        if(all.isEmpty) -1L else all.min
    }
}
