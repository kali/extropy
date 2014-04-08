package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
import akka.util.{ ByteString, ByteIterator }

import mongo.{ Message, Change, CraftedMessage }

sealed abstract class Direction
object Server extends Direction
object Client extends Direction

case class TargettedMessage(direction:Direction, message:Message)

object ExtropyProxy {
    def props(extropy:BaseExtropyContext) = Props(classOf[ExtropyProxy], extropy, None)
    def props(extropy:BaseExtropyContext, config:DynamicConfiguration) =
        Props(classOf[ExtropyProxy], extropy, Some(config))
}

class ExtropyProxy( extropy:BaseExtropyContext,
                    initialConfiguration:Option[DynamicConfiguration]) extends Actor {

    var configuration:DynamicConfiguration = initialConfiguration.getOrElse(extropy.pullConfiguration)

    def receive = {
        case c:DynamicConfiguration => {
            configuration = c
            sender ! AckDynamicConfiguration(c)
        }
        case msg@TargettedMessage(Client,_) => sender ! msg
        case msg@TargettedMessage(Server,mongo) =>
            if(mongo.isChange) {
                val originalChange:Change = mongo.op.asChange
                val alteredChange:Change = configuration.invariants.foldLeft(originalChange) { (change,inv) =>
                    if(inv.monitoredCollections.contains(change.writtenCollection))
                        inv.alterWrite(change)
                    else
                        change
                }
                if(alteredChange != originalChange)
                    sender ! TargettedMessage(Server,
                        CraftedMessage(mongo.header.requestId, mongo.header.responseTo, mongo.op.adoptChange(alteredChange))
                    )
                else
                    sender ! msg
            } else
                sender ! msg
    }
}
