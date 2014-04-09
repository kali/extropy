package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }

object Worker {
    def props(extropy:BaseExtropyContext, name:String) =
        Props(classOf[Worker], extropy, name)
}

class Worker(extropy:BaseExtropyContext, name:String) extends Actor {
    import context.system

    val agent = context.actorOf(ExtropyAgent.props(s"worker-$name", extropy, self), "agent")
    var configuration = extropy.pullConfiguration

    def receive = {
        case c:DynamicConfiguration => {
            configuration = c
            sender ! AckDynamicConfiguration(c)
        }
    }
}
