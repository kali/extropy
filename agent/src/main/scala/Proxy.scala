package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
import akka.util.{ ByteString, ByteIterator }

import mongo._

import com.mongodb.casbah.Imports._

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

    var logic = ExtropyProxyLogic(extropy, optionalConfiguration)

    var responseId:Int = 0
    var processing:Iterable[AnalysedChange] = Iterable()

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
        val query = op.query
        val command = if(query.keySet.isEmpty)
            "status"
        else
            query.keySet.iterator.next
        command match {
            case "status" => OpReply(0, 0, 0, 1, Stream(MongoDBObject("ok" -> 1)))
            case "configVersion" => OpReply(0, 0, 0, 1,
                            Stream(MongoDBObject("ok" -> 1, "version" -> logic.configuration.version)))
        }
    }
}
