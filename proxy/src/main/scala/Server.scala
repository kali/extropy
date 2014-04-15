package org.zoy.kali.extropy

import akka.actor.{ ActorSystem, Actor, ActorRef, Props, Terminated }
import akka.io.{ IO, Tcp }
import akka.util.{ ByteString, ByteIterator }
import java.net.InetSocketAddress
import akka.event.Logging
import scala.collection.mutable.Buffer

import org.bson.BSONObject

import com.mongodb.casbah.Imports._

import org.kohsuke.args4j.{ Option => Args4jOption, CmdLineParser, CmdLineException }

class RunDefinition {
    @Args4jOption(  name="--payload", aliases=Array("-p"),
                    usage="""payload mongodb server, defaults to localhost:27017""",
                    metaVar="host:port")
    var payloadServer = "localhost:27017"

    @Args4jOption(  name="--extropy", aliases=Array("-e"),
                    usage="""exproxy mongo database, defaults to <payload-server>/extropy""",
                    metaVar="host:port/database")
    var extropyServer = ""

    @Args4jOption(  name="--listen", aliases=Array("-l"),
                    usage="""where the  proxy should listen to "[ip:]port"""", metaVar="[ip:]port")
    var listenTo = "127.0.0.1:27000"

    @Args4jOption(name="--noproxy", aliases=Array(), usage="""Start a proxy""")
    var noListen:Boolean = false

    @Args4jOption(name="--worker", aliases=Array(), usage="Start a worker")
    var worker:Boolean = true

    lazy val listenHost = if(listenTo.contains(":")) listenTo.replaceAll(":.*","") else "localhost"
    lazy val listenPort = if(listenTo.contains(":")) listenTo.replaceAll(".*:","").toInt else 27017

    lazy val payloadHost = payloadServer.split(':').head
    lazy val payloadPort = payloadServer.split(':').last.toInt
    lazy val payloadMongo = MongoClient(payloadHost, payloadPort)

    lazy val extropyDatabase = if(extropyServer == "" || !extropyServer.contains("/")) "extropy" else extropyServer.split("/").last
    lazy val extropyHostAndPort = if(extropyServer.contains("/")) extropyServer.replaceAll("/.*", "") else payloadServer
    lazy val extropyHost = extropyHostAndPort.split(':').head
    lazy val extropyPort = extropyHostAndPort.split(':').last.toInt

    lazy val extropyMongo = if(payloadServer == extropyServer)
        payloadMongo
    else
        MongoClient(extropyHost, extropyPort)

    def run {
        val system = ActorSystem("extropy-proxy")
        Runtime.getRuntime.addShutdownHook(new Thread() {
            override def run() {
                system.shutdown()
                System.err.println("Shutdown, awaiting akka system termination")
                system.awaitTermination()
            }
        })

        val extropy = Extropy(extropyMongo(extropyDatabase), payloadMongo)

        if(worker) {
            system.actorOf(Overseer.props(extropy, extropy.hostname.replace(".", "-")), "overseer")
        }
        if(!noListen) {
            system.actorOf(
                ProxyServer.props(extropy, new InetSocketAddress(listenHost, listenPort),
                    new InetSocketAddress(payloadHost, payloadPort)), "proxyListener"
            )
        }
    }
}

object Boot {
    def main(args:Array[String]) {
        val definition = new RunDefinition
        val parser = new CmdLineParser(definition)
        try {
            parser.parseArgument(args:_*)
            definition.run
        } catch { case e:Throwable =>
            System.err.println(e.getMessage)
            parser.printUsage(System.err)
        }
    }
}

// this is the TCP socket/dispatcher
object ProxyServer {
    def props(extropy:BaseExtropyContext, bind:InetSocketAddress, send:InetSocketAddress) =
        Props(classOf[ProxyServer], extropy, bind, send)
}

class ProxyServer(val extropy:BaseExtropyContext, val bind:InetSocketAddress, val send:InetSocketAddress)
        extends Actor {

    import Tcp._
    import context.system

    val log = Logging(context.system, this)

    log.info(s"Setup proxy from $bind to $send")

    var configuration = extropy.pullConfiguration
    val agent = context.actorOf(ExtropyAgent.props("proxy-" + extropy.hostname + "/" + bind.toString, extropy, self), "agent")

    import scala.collection.mutable.Set
    val pendingAcknowledgement:Set[ActorRef] = Set()

    IO(Tcp) ! Bind(self, bind)

    def receive = {
        case b @ Bound(localAddress) =>
            log.info(s"bound to $localAddress")

        case CommandFailed(_: Bind) => context stop self

        case c @ Connected(remote, local) => {
            val kid = context.actorOf(ProxyPipe.props(extropy, sender, send),
                                    "proxy-for-" + remote.getHostName +  ":" + remote.getPort)
            context watch kid
        }

        case c:DynamicConfiguration => {
            configuration = c
            pendingAcknowledgement.clear
            context.children.filter( _.path.name.startsWith("proxy-for") ).foreach { kid =>
                pendingAcknowledgement.add(kid)
                kid ! c
            }
        }

        case a:AckDynamicConfiguration => {
            if(a.config == configuration) {
                pendingAcknowledgement.remove(sender)
                if(pendingAcknowledgement.isEmpty)
                    agent ! a
            }
        }

        case Terminated(kid) => {
            pendingAcknowledgement.remove(kid)
        }


    }
}

object ProxyPipe {
    def props(extropy:BaseExtropyContext, incomingFrontendConnection:ActorRef, backendAddress:InetSocketAddress) =
        Props(classOf[ProxyPipe], extropy, incomingFrontendConnection, backendAddress)
}

class ProxyPipe(extropy:BaseExtropyContext, socket:ActorRef, backendAddress:InetSocketAddress) extends Actor {
    import Tcp._
    import context.system

    val log = Logging(context.system, this)
    val frontendHandler = context.actorOf(ConnectionActor.props(socket), "frontend")
    val backendHandler = context.actorOf(ConnectionActor.props(backendAddress), "backend")
    val proxy = context.actorOf(ExtropyProxy.props(extropy), "proxy")

    context watch frontendHandler
    context watch backendHandler

    def receive = {
        case Terminated(_) => context stop self
        case msg:ByteString if(sender == frontendHandler) =>
            proxy ! TargettedMessage(Server, mongo.IncomingMessage(msg))
        case msg:ByteString if(sender == backendHandler) =>
            proxy ! TargettedMessage(Client, mongo.IncomingMessage(msg))
        case TargettedMessage(Client, msg) => frontendHandler ! msg.binary
        case TargettedMessage(Server, msg) => backendHandler ! msg.binary
        case msg:DynamicConfiguration => proxy ! msg
        case msg:AckDynamicConfiguration => context.parent ! msg
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
            readBuffer ++= data
            splitReadBuffer
        case data:ByteString =>
            writeBuffer += data
            writeSome
        case Ack =>
            waitingAck = false
            writeSome
        case CommandFailed =>
              context stop self
        case c @ Connected(remote, local) =>
            socket = sender
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
            context.parent ! data
            readBuffer = readBuffer.drop(data.length)
        }
    }
}
