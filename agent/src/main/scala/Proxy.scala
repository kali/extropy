/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
import akka.event.Logging
import akka.util.{ ByteString, ByteIterator }

import mongo._
import mongoutils.BSONObjectConversions._

import org.bson.BSONObject
import com.mongodb.casbah.Imports._

import com.novus.salat._
import custom.ctx

sealed abstract class Direction
object Server extends Direction
object Client extends Direction

case class TargettedMessage(direction:Direction, message:Message)

object ExtropyProxyActor {
    def props(extropy:BaseExtropyContext) = Props(classOf[ExtropyProxyActor], extropy, None)
    def props(extropy:BaseExtropyContext, config:DynamicConfiguration) =
        Props(classOf[ExtropyProxyActor], extropy, Some(config))
}

class ExtropyProxyActor(    extropy:BaseExtropyContext,
                            optionalConfiguration:Option[DynamicConfiguration]) extends Actor {

    val log = Logging(context.system, this)
    var logic = ExtropyProxyLogic(extropy, optionalConfiguration)

    var responseId:Int = 0
    var processing:Iterable[ResolvableChange] = Iterable()

    def nextResponseId = { responseId+=1 ; responseId }

    def receive = {
        case c:DynamicConfiguration => {
            logic = ExtropyProxyLogic(extropy, Some(c))
            sender ! AckDynamicConfiguration(logic.configuration)
        }
        case msg@TargettedMessage(Client,mongo) =>
            processing.foreach( logic.postChange(_) )
            processing = Iterable()
            sender ! msg
        case msg@TargettedMessage(Server, op) if(op.op.isExtropyCommand) => {
            sender ! TargettedMessage(Client,
                        CraftedMessage(nextResponseId, op.header.requestId,
                            handleExtropyCommand(op.op.asInstanceOf[OpQuery])))
        }
        case msg@TargettedMessage(Server,mongo) =>
            if(mongo.isChange) {
                processing = mongo.op.asChanges.map( logic.preChange( _ ) )
/*
                if(processing.alteredChange != originalChange)
                    sender ! TargettedMessage(Server,
                        CraftedMessage(mongo.header.requestId, mongo.header.responseTo,
                            mongo.op.adoptChange(processing.alteredChange))
                    )
                else
*/
                    sender ! msg
            } else
                sender ! msg
    }

    def handleExtropyCommand(op:OpQuery):OpReply = {
        try {
            val command = op.query.keys.headOption.getOrElse("status")
            command match {
                case "status" => OpReply(0, 0, 0, 1, Stream(MongoDBObject("ok" -> 1)))
                case "configVersion" => OpReply(0, 0, 0, 1,
                                Stream(MongoDBObject("ok" -> 1, "version" -> logic.configuration.version)))
                case "addInvariant" =>
                    extropy.invariantDAO.salat.collection.insert(op.query.as[BSONObject]("addInvariant"))
                    extropy.agentDAO.bumpConfigurationVersion
                    OpReply(0, 0, 0, 1, Stream(MongoDBObject("ok" -> 1)))
                case "addRule" =>
                    extropy.invariantDAO.salat.insert(Invariant(Rule.fromMongo(op.query.as[BSONObject]("addRule"))))
                    extropy.agentDAO.bumpConfigurationVersion
                    OpReply(0, 0, 0, 1, Stream(MongoDBObject("ok" -> 1)))
                case _ => OpReply(0, 0, 0, 1, Stream(MongoDBObject("ok" -> 0, "message" -> "unknown command")))
            }
        } catch {
                case e:Throwable =>
                    log.error(e.toString, e)
                    OpReply(0, 0, 0, 1, Stream(MongoDBObject("ok" -> 0, "message" -> e.toString)))
        }
    }
}
