package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated, PoisonPill }
import akka.event.Logging

import scala.concurrent.duration._

import mongoutils._

class ExtropyAgent(val id:String, val extropy:BaseExtropyContext, val client:ActorRef) extends Actor {
    object Ping
    val logger = Logging(context.system, this)
    val pings = context.system.scheduler.schedule(0.milliseconds, extropy.agentHeartBeat,
                    self, Ping)(executor=context.system.dispatcher)

    context watch client

    var configuration:DynamicConfiguration = extropy.pullConfiguration

    def ping {
        var wanted = extropy.agentDAO.readConfigurationVersion
        logger.debug(s"ping wanted:$wanted have:${configuration.version}")
        if(wanted != configuration.version) {
            while(wanted > configuration.version) {
                configuration = extropy.pullConfiguration
                wanted = extropy.agentDAO.readConfigurationVersion
        logger.debug(s"pulled:${configuration.version}, now want:$wanted")
            }
            client ! configuration
        }
        extropy.agentDAO.ping(id, extropy.agentLockDuration)
    }

    def receive = {
        case Ping => ping
        case AckDynamicConfiguration(dc:DynamicConfiguration) => extropy.agentDAO.ackVersion(id, dc.version)
        case Terminated(_) => pings.cancel
        case PoisonPill => pings.cancel
    }

    override def postStop = {
        pings.cancel
        extropy.agentDAO.unregister(id)
        super.postStop
    }

}

object ExtropyAgent {
    def props(id:String, extropy:BaseExtropyContext, client:ActorRef) = {
        extropy.agentDAO.register(id, extropy.agentDAO.readConfigurationVersion)
        Props(classOf[ExtropyAgent], id, extropy, client)
    }
}

