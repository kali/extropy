package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated, PoisonPill }
import akka.event.Logging

import scala.concurrent.duration._
import com.mongodb.casbah.Imports._
import mongo.MongoLockingPool.LockerIdentity

object Overseer {
    def props(extropy:BaseExtropyContext, name:String) =
        Props(classOf[Overseer], extropy, name)
}

class Overseer(extropy:BaseExtropyContext, name:String) extends Actor {
    import context.system
    object HeartBeat
    implicit val locker = LockerIdentity(name)

    val log = Logging(context.system, this)
    val pings = context.system.scheduler.schedule(0 milliseconds, extropy.overseerHeartBeat,
                    self, HeartBeat)(executor=context.system.dispatcher)

    val agent = context.actorOf(ExtropyAgent.props(s"worker-$name", extropy, self), "agent")

    def receive = {
        case c:DynamicConfiguration => sender ! AckDynamicConfiguration(c)
        case PoisonPill =>
            context.actorSelection("./*") ! PoisonPill
            pings.cancel
        case HeartBeat => extropy.invariantDAO.prospect.foreach { inv =>
            context.actorOf(Foreman.props(extropy, inv, locker), "foreman-" + inv.id.toString)
        }
    }

    override def postStop = {
        pings.cancel
        super.postStop
    }
}

object Foreman {
    def props(extropy:BaseExtropyContext, invariant:Invariant, locker:LockerIdentity) =
        Props(classOf[Foreman], extropy, invariant, locker)
}

class Foreman(extropy:BaseExtropyContext, invariant:Invariant, implicit val locker:LockerIdentity) extends Actor {
    object Ping
    val pings = context.system.scheduler.schedule(0 milliseconds, extropy.foremanHeartBeat,
                    self, Ping)(executor=context.system.dispatcher)

    val log = Logging(context.system, this)
    log.info("Claiming " + invariant)

    def receive = {
        case Ping =>
    }
}
