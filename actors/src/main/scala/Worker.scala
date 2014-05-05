package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated, PoisonPill, FSM }
import akka.event.Logging
import akka.pattern.pipe

import scala.concurrent.duration._
import com.mongodb.casbah.Imports._
import scala.concurrent.ExecutionContext.Implicits.global
import mongoutils._

object Overseer {
    def props(extropy:BaseExtropyContext, name:String) =
        Props(classOf[Overseer], extropy, name)
}

class Overseer(extropy:BaseExtropyContext, name:String) extends Actor {
    import context.system
    object HeartBeat
    val id = s"worker-$name"
    implicit val locker = LockerIdentity(id)

    val log = Logging(context.system, this)
    log.info(s"Setup worker: $id")
    val pings = context.system.scheduler.schedule(0 milliseconds, extropy.overseerHeartBeat,
                    self, HeartBeat)(executor=context.system.dispatcher)

    val agent = context.actorOf(ExtropyAgent.props(id, extropy, self), "agent")

    def receive = {
        case c:DynamicConfiguration => sender ! AckDynamicConfiguration(c)
        case PoisonPill =>
            context.actorSelection("./*") ! PoisonPill
            pings.cancel
        case HeartBeat => extropy.prospect.foreach { inv =>
            context.actorOf(Foreman.props(extropy, inv, locker), "foreman-" + inv._id.toString)
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

class Foreman(extropy:BaseExtropyContext, var invariant:Invariant, implicit val locker:LockerIdentity) extends Actor {
    object Ping
    object SyncDone
    val pings = context.system.scheduler.schedule(0 milliseconds, extropy.foremanHeartBeat,
                    self, Ping)(executor=context.system.dispatcher)

    val log = Logging(context.system, this)
    var expectedVersion = 0L

    import InvariantStatus._
    def receive = {
        case t:Throwable => log.error("During active sync: " + t.getMessage)
        case SyncDone => expectedVersion = extropy.switchInvariantTo(invariant, Run)
        case Ping => try {
                invariant = extropy.claim(invariant)
            } catch {
                case e:IllegalStateException => {
                    println("suicide after failure to relock my invariant: " + invariant)
                    self ! PoisonPill
                }
            }
            invariant.command.foreach { s =>
                extropy.switchInvariantTo(invariant, s)
                extropy.ackCommand(invariant)
            }
            val statusChange = invariant.statusChanging && extropy.agentDAO.readMinimumConfigurationVersion >= expectedVersion
            if(statusChange)
                extropy.ackStatusChange(invariant)

            invariant.status match {
                case Created => expectedVersion = extropy.switchInvariantTo(invariant, Sync)
                case Sync if(statusChange) =>
                        scala.concurrent.Future {
                            invariant.rule.fixAll(extropy.payloadMongo)
                            log.info("Done sync")
                            SyncDone
                        }.pipeTo(self)
                case Sync    => // FIXME: report on progress, if possible
                case Run     =>
                case Stop    =>
                case Error   =>
            }

    }
}
