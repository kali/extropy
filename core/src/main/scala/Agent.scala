package org.zoy.kali.extropy

import java.util.Date
import com.mongodb.casbah.Imports._

import akka.actor.{ ActorSystem, Actor, Props }

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
        collection.update(MongoDBObject("_id" -> id), MongoDBObject("_id" -> id,
            "until" -> until), upsert=true)
    }

    def removeById(id:String) {
        remove(MongoDBObject("_id" -> id))
    }
}

class ConfigurationVersionHolderDAO(val db:MongoDB) {
    val collection = db("configuration_version")
    def read:Long = collection.findOne(MongoDBObject("_id" -> "version")).flatMap( d => d.getAs[Long]("value") ).getOrElse(0L)
    def bump:Long = collection.findAndModify(
            query=MongoDBObject("_id" -> "version"),
            update=MongoDBObject("$inc" -> MongoDBObject("value" -> 1L)),
            sort=null, fields=null, upsert=true, remove=false, returnNew=true).flatMap( _.getAs[Long]("value") ).get
}

class ExtropyAgent( val id:String, val extropyAgentDao:ExtropyAgentDescriptionDAO,
                    val pingHeartBeat:FiniteDuration, val pingValidity:FiniteDuration) extends Actor {
    object Ping {}
    val pings = context.system.scheduler.schedule(0 milliseconds, pingHeartBeat,
                    self, Ping)(executor=context.system.dispatcher)

    def receive = {
        case Ping => extropyAgentDao.ping(id, pingValidity)
    }

    override def postStop = {
        super.postStop
        pings.cancel
        extropyAgentDao.removeById(id)
    }

}

object ExtropyAgent {
    def props(id:String, extropyAgentDao:ExtropyAgentDescriptionDAO) = Props(classOf[ExtropyAgent], id,
                extropyAgentDao, 250 milliseconds, 2500 milliseconds)
}
