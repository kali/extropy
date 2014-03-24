package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
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
                                new InetSocketAddress("localhost", 27017)
            ), "proxyServer")
    }
}

// this is the TCP socket/dispatcher
object ProxyServer {
    def props(invariants:Seq[Invariant], bind:InetSocketAddress, send:InetSocketAddress) =
        Props(classOf[ProxyServer], invariants, bind, send)
}

class ProxyServer(val invariants:Seq[Invariant], val bind:InetSocketAddress, val send:InetSocketAddress)
        extends Actor {

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
            context.actorOf(ProxyPipe.props(sender, send),
                                    "worker-" + remote.getHostName +  ":" + remote.getPort)

    }
}

object ProxyPipe {
    def props(incomingFrontendConnection:ActorRef, backendAddress:InetSocketAddress) =
        Props(classOf[ProxyPipe], incomingFrontendConnection, backendAddress)
}

class ProxyPipe(socket:ActorRef, backendAddress:InetSocketAddress) extends Actor {
    import Tcp._
    import context.system

    val log = Logging(context.system, this)
    val frontendHandler = context.actorOf(ConnectionActor.props(socket), "frontend")
    val backendHandler = context.actorOf(ConnectionActor.props(backendAddress), "backend")
    val proxy = context.actorOf(ExtropyProxy.props(List()), "proxy")

    context watch frontendHandler
    context watch backendHandler

    def receive = {
        case Terminated(_) => context stop self
        case msg:ByteString if(sender == frontendHandler) =>
            proxy ! TargettedMessage(Server, IncomingMessage(msg))
        case msg:ByteString if(sender == backendHandler) =>
            proxy ! TargettedMessage(Client, IncomingMessage(msg))
        case TargettedMessage(Client, msg) => frontendHandler ! msg.binary
        case TargettedMessage(Server, msg) => backendHandler ! msg.binary
    }
}

// this is the client
object ConnectionActor {
    def props(connection:ActorRef) = Props(classOf[ConnectionActor], connection)
    def props(backend:InetSocketAddress) = Props(classOf[ConnectionActor], backend)
}

class ConnectionActor extends Actor {

    import Tcp._
    import context.system

    val log = Logging(context.system, this)

    def this(backend:InetSocketAddress) {
        this()
        log.debug(s"opening connection to $backend")
        IO(Tcp) ! Connect(backend)
    }

    def this(s:ActorRef) {
        this()
        socket = s
        log.debug(s"accepting connection $socket")
        socket ! Register(self)
        context watch socket
    }

    var socket:ActorRef = null
    var readBuffer:ByteString = ByteString.empty
    val writeBuffer:Buffer[ByteString] = Buffer()
    var waitingAck:Boolean = false

    case object Ack extends Event

    def receive = {
        case Received(data) =>
log.debug(s"received $data")
            readBuffer ++= data
            splitReadBuffer
        case data:ByteString =>
log.debug(s"about to send $data")
            writeBuffer += data
            writeSome
        case Ack =>
            waitingAck = false
            writeSome
        case CommandFailed =>
              context stop self
        case c @ Connected(remote, local) =>
            socket = sender()
            log.debug(s"connected to $socket")
            socket ! Register(self)
            context watch socket
            writeSome
        case t@Terminated(_) => context stop self
    }

    def writeSome {
        if(!writeBuffer.isEmpty && socket != null && !waitingAck) {
            socket ! Write(writeBuffer.head, Ack)
            writeBuffer.trimStart(1)
            waitingAck = true
        }
    }

    def splitReadBuffer = {
        implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
        while(readBuffer.length > 4 && readBuffer.iterator.getInt <= readBuffer.length) {
            val data = readBuffer.take(readBuffer.iterator.getInt)
log.debug(s"forward $data to ${context.parent}")
            context.parent ! data
            readBuffer = readBuffer.drop(data.length)
        }
    }
}
