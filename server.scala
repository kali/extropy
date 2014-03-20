
import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.event.Logging


object Boot {
    def main(args:Array[String]) {
        val system = ActorSystem("extropy-proxy")
        val server = system.actorOf(Props[Server], "server")
    }
}

// this is the TCP socket/dispatcher
class Server extends Actor {

    import Tcp._
    import context.system

    val log = Logging(context.system, this)

    IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 27000))

    def receive = {
        case b @ Bound(localAddress) => log.info(s"bound to $localAddress")

        case CommandFailed(_: Bind) => context stop self

        case c @ Connected(remote, local) =>
            val socket = sender()
            val handler = context.actorOf(IncomingConnectionActor.props(socket))
            socket ! Register(handler)
    }
}

// this is the TCP server socket handler
object IncomingConnectionActor {
    def props(socket:ActorRef) = Props(classOf[IncomingConnectionActor], socket)
}

class IncomingConnectionActor(socket:ActorRef) extends Actor {
    import Tcp._
    val client = context.actorOf(Client.props(self))

    var buffer:ByteString = ByteString.empty

    def receive = {
        case Received(data) => {
            println("> " + data)
            buffer = buffer ++ data
            splitAndSend
        }
        case PeerClosed     => context stop self
        case data: ByteString => {
            println("< " + data)
            socket ! Write(data)
        }
    }

    def splitAndSend = {
        implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
        while(buffer.length > 4 && buffer.iterator.getInt <= buffer.length) {
            val data = buffer.take(buffer.iterator.getInt)
            client ! data
            buffer = buffer.drop(data.length)
        }
    }
}

// this is the backend client
object Client {
    def props(incoming:ActorRef) = Props(classOf[Client], incoming)
}

class Client(incoming: ActorRef) extends Actor {

    import Tcp._
    import context.system

    IO(Tcp) ! Connect(new InetSocketAddress("www.virtual.ftnz.net", 27017))

    var socket:ActorRef = null
    var buffer:ByteString = ByteString.empty

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
        case data: ByteString =>
            socket ! Write(data)
        case CommandFailed(w: Write) =>
        // O/S buffer was full
            socket ! "write failed"
        case Received(data) =>
            buffer ++= data
            splitAndSend
        case "close" =>
            socket ! Close
        case _: ConnectionClosed =>
            socket ! "socket closed"
            context stop self
    }

    def splitAndSend = {
        implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
        while(buffer.length > 4 && buffer.iterator.getInt <= buffer.length) {
            val data = buffer.take(buffer.iterator.getInt)
            incoming ! data
            buffer = buffer.drop(data.length)
        }
    }
}

