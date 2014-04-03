package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
import akka.util.{ ByteString, ByteIterator }

sealed abstract class Direction
object Server extends Direction
object Client extends Direction

case class TargettedMessage(direction:Direction, message:mongo.Message)

object ExtropyProxy {
    def props(invariants:List[Invariant]) = Props(classOf[ExtropyProxy], invariants)
}

class ExtropyProxy(val invariants:List[Invariant]) extends Actor {
    def receive = {
        case msg@TargettedMessage(Client,_) => sender ! msg
        case msg@TargettedMessage(Server,_) => {
            invariants.foreach( _.preprocess(msg) )
            sender ! msg
        }
    }
}
