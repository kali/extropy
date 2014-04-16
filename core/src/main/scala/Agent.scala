package org.zoy.kali.extropy

import java.util.Date
import com.mongodb.casbah.Imports._

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

import mongoutils._

case class ExtropyAgentDescription(_id:String, emlp:MongoLock, configurationVersion:Long=(-1L))

class ExtropyAgentDescriptionDAO(val db:MongoDB, val pingValidity:FiniteDuration)(implicit ctx: com.novus.salat.Context) {
    val collection = db("agents")
    val salat = new SalatDAO[ExtropyAgentDescription,ObjectId](collection) {}

    val agentMLP = MongoLockingPool(collection, pingValidity)

    def register(id:String, configurationVersion:Long) {
        agentMLP.cleanupOldLocks
        agentMLP.insertLocked(MongoDBObject("_id" -> id, "configurationVersion" -> configurationVersion))(LockerIdentity(id))
    }

    def ping(id:String, validity:FiniteDuration) {
        agentMLP.relock(MongoDBObject("_id" -> id), validity)(LockerIdentity(id))
    }

    def unregister(id:String) {
        agentMLP.release(MongoDBObject("_id" -> id), delete=true)(LockerIdentity(id))
    }

    def ackVersion(id:String, version:Long) {
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
