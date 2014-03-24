package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
import akka.util.{ ByteString, ByteIterator }

sealed abstract class Direction
object Server extends Direction
object Client extends Direction

sealed abstract class Message {
    def binary:ByteString
    def op:MessageParser.Op
}

case class IncomingMessage(val binary:ByteString) extends Message {
    lazy val op = MessageParser.parse(binary)
}

case class GeneratedMessage(val op:MessageParser.Op) extends Message {
    lazy val binary = op.toBinary
}

case class TargettedMessage(direction:Direction, message:Message)

object ExtropyProxy {
    def props(invariants:List[Invariant]) = Props(classOf[ExtropyProxy], invariants)
}

class ExtropyProxy(val invariants:List[Invariant]) extends Actor {
    def receive = {
        case msg:TargettedMessage => context.parent ! msg
    }
}
