package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.io.{ IO, Tcp }
import akka.util.{ ByteString, ByteIterator }
import java.net.InetSocketAddress
import akka.event.Logging
import scala.collection.mutable.Buffer

import org.bson.BSONObject

object Boot {
    def main(args:Array[String]) {
        val system = ActorSystem("extropy-proxy")
        val server = system.actorOf(
            ProxyServer.props(  List(StringNormalizationInvariant("name", "normName")),
                                new InetSocketAddress("localhost", 27000),
                                new InetSocketAddress("www.virtual.ftnz.net", 27017)
            ), "proxyServer")
    }
}

// this is the TCP socket/dispatcher
object ProxyServer {
    def props(invariants:Seq[Invariant], bind:InetSocketAddress, send:InetSocketAddress) =
        Props(classOf[ProxyServer], invariants, bind, send)
}

class ProxyServer(val invariants:Seq[Invariant], val bind:InetSocketAddress, val send:InetSocketAddress) extends Actor {

    import Tcp._
    import context.system

    val log = Logging(context.system, this)

    log.info(s"Setup proxy from $bind to $send")
    IO(Tcp) ! Bind(self, bind)

    def receive = {
        case b @ Bound(localAddress) =>
            log.info(s"bound to $localAddress")

        case CommandFailed(_: Bind) => context stop self

        case c @ Connected(remote, local) =>
            val socket = sender()
            val handler = context.actorOf(FrontendConnectionActor.props(socket, send), "frontend-" + remote.getHostName +  ":" + remote.getPort)
            socket ! Register(handler)
    }
}

// this is the TCP server socket handler
object FrontendConnectionActor {
    def props(socket:ActorRef, backend:InetSocketAddress) = Props(classOf[FrontendConnectionActor], socket, backend)
}

class FrontendConnectionActor(socket:ActorRef, backend:InetSocketAddress) extends Actor {
    import Tcp._
    val log = Logging(context.system, this)
    log.info(s"accepting connection")

    val client = context.actorOf(BackendConnectionActor.props(self, backend), "backend")

    var incomingBuffer:ByteString = ByteString.empty

    case object Ack extends Event
    val writeBuffer:Buffer[ByteString] = Buffer()
    var waitingAck:Boolean = false

    def receive = {
        case Received(data) =>
            log.debug("> " + MessageParser.parse(data))
            incomingBuffer ++= data
            splitAndSend

        case PeerClosed     => context stop self
        case data:ByteString =>
            log.debug("< " + MessageParser.parse(data))
            if(!waitingAck) {
                socket ! Write(data, Ack)
                waitingAck = true
            } else
                writeBuffer += data
        case Ack =>
            if(writeBuffer.isEmpty)
                waitingAck = false
            else {
                socket ! Write(writeBuffer.head, Ack)
                writeBuffer.trimStart(1)
            }
    }

    def splitAndSend = {
        implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
        while(incomingBuffer.length > 4 && incomingBuffer.iterator.getInt <= incomingBuffer.length) {
            val data = incomingBuffer.take(incomingBuffer.iterator.getInt)
            client ! data
            incomingBuffer = incomingBuffer.drop(data.length)
        }
    }
}

// this is the backend client
object BackendConnectionActor {
    def props(incoming:ActorRef, backend:InetSocketAddress) = Props(classOf[BackendConnectionActor], incoming, backend:InetSocketAddress)
}

class BackendConnectionActor(incoming: ActorRef, backend:InetSocketAddress) extends Actor {

    import Tcp._
    import context.system

    val log = Logging(context.system, this)
    log.info(s"opening connection")

    IO(Tcp) ! Connect(backend)

    var socket:ActorRef = null
    var readBuffer:ByteString = ByteString.empty
    val writeBuffer:Buffer[ByteString] = Buffer()
    var waitingAck:Boolean = false

    case object Ack extends Event

    def receive = {
        case CommandFailed(_: Connect) =>
              incoming ! "connect failed"
              context stop self
        case c @ Connected(remote, local) =>
            incoming ! c
            socket = sender()
            socket ! Register(self)
            context become active
    }

    def active:Receive = {
        case Received(data) =>
            readBuffer ++= data
            splitReadBuffer
        case data:ByteString =>
            if(waitingAck)
                writeBuffer += data
            else {
                socket ! Write(data, Ack)
                waitingAck = true
            }
        case Ack => {
            if(writeBuffer.isEmpty)
                waitingAck = false
            else {
                socket ! Write(writeBuffer.head, Ack)
                writeBuffer.trimStart(1)
            }
        }
        case CommandFailed(w: Write) => socket ! "write failed" // should not happen
        case "close" => socket ! Close
        case _: ConnectionClosed =>
            socket ! "socket closed"
            context stop self
    }

    def splitReadBuffer = {
        implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
        while(readBuffer.length > 4 && readBuffer.iterator.getInt <= readBuffer.length) {
            val data = readBuffer.take(readBuffer.iterator.getInt)
            incoming ! data
            readBuffer = readBuffer.drop(data.length)
        }
    }
}

