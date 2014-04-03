package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
import akka.util.{ ByteString, ByteIterator }

import mongo.{ Message, WriteOp, CraftedMessage }

sealed abstract class Direction
object Server extends Direction
object Client extends Direction

case class TargettedMessage(direction:Direction, message:Message)

object ExtropyProxy {
    def props(invariants:List[Invariant]) = Props(classOf[ExtropyProxy], invariants)
}

class ExtropyProxy(val invariants:List[Invariant]) extends Actor {
    def receive = {
        case msg@TargettedMessage(Client,_) => sender ! msg
        case msg@TargettedMessage(Server,mongo) =>
            if(mongo.isWriteOp) {
                val originalOp:WriteOp = mongo.op.asWriteOp
                val alteredOp:WriteOp = invariants.foldLeft(originalOp) { (op,inv) =>
                    if(inv.monitoredCollections.contains(op.writtenCollection))
                        inv.alterWrite(op)
                    else
                        op
                }
                if(alteredOp != originalOp)
                    sender ! TargettedMessage(Server,
                        CraftedMessage(mongo.header.requestId, mongo.header.responseTo, alteredOp)
                    )
                else
                    sender ! msg
            } else
                sender ! msg
    }
}
