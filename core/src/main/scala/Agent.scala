package org.zoy.kali.extropy

import java.util.Date
import com.mongodb.casbah.Imports._

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._

import com.mongodb.casbah.Imports._

import com.novus.salat.global._

import scala.concurrent.duration._

import org.zoy.kali.extropy.models.ExtropyAgentDescription

class ExtropyAgentDescriptionDAO(val db:MongoDB) extends SalatDAO[ExtropyAgentDescription,ObjectId](db("agents")) {

    def ping(id:String, validity:FiniteDuration) {
        val until = new Date(validity.fromNow.time.toMillis)
        collection.update(MongoDBObject("_id" -> id),
            MongoDBObject("$set" -> MongoDBObject("until" -> until)),
        upsert=true)
    }

    def ackVersion(id:String, version:Long) {
        collection.update(MongoDBObject("_id" -> id),
            MongoDBObject("$set" -> MongoDBObject("configurationVersion" -> version)),
        upsert=true)
    }

    def removeById(id:String) {
        remove(MongoDBObject("_id" -> id))
    }

    val versionCollection = db("configuration_version")
    def readConfigurationVersion:Long = versionCollection.findOne(MongoDBObject("_id" -> "version"))
                        .flatMap( d => d.getAs[Long]("value") ).getOrElse(0L)
    def bumpConfigurationVersion:Long = versionCollection.findAndModify(
            query=MongoDBObject("_id" -> "version"),
            update=MongoDBObject("$inc" -> MongoDBObject("value" -> 1L)),
            sort=null, fields=null, upsert=true, remove=false, returnNew=true).flatMap( _.getAs[Long]("value") ).get
}

class ExtropyAgent(val id:String, val extropy:BaseExtropyContext, val client:ActorRef) extends Actor {
    object Ping {}
    val pings = context.system.scheduler.schedule(0 milliseconds, extropy.pingHeartBeat,
                    self, Ping)(executor=context.system.dispatcher)

    var configuration:DynamicConfiguration = extropy.pullConfiguration

    def ping {
        var wanted = extropy.agentDAO.readConfigurationVersion
        if(wanted != configuration.version) {
            while(wanted > configuration.version) {
                configuration = DynamicConfiguration(wanted, extropy.invariantDAO.find(MongoDBObject.empty).toList)
                wanted = extropy.agentDAO.readConfigurationVersion
            }
            client ! configuration
        }
        extropy.agentDAO.ping(id, extropy.pingValidity)
    }

    def receive = {
        case Ping => ping
        case AckDynamicConfiguration(dc:DynamicConfiguration) => extropy.agentDAO.ackVersion(id, dc.version)
    }

    override def postStop = {
        super.postStop
        pings.cancel
        extropy.agentDAO.removeById(id)
    }

}

object ExtropyAgent {
    def props(id:String, extropy:BaseExtropyContext, client:ActorRef) = Props(classOf[ExtropyAgent], id, extropy, client)
}
