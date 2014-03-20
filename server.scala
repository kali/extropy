
import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
import akka.io.{ IO, Tcp }
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.event.Logging
import scala.collection.mutable.Buffer


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

    var incomingBuffer:ByteString = ByteString.empty

    case object Ack extends Event
    val writeBuffer:Buffer[ByteString] = Buffer()
    var waitingAck:Boolean = false

    def receive = {
        case Received(data) =>
            incomingBuffer ++= data
            splitAndSend

        case PeerClosed     => context stop self
        case data:ByteString =>
            println("< " + data)
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
object Client {
    def props(incoming:ActorRef) = Props(classOf[Client], incoming)
}

class Client(incoming: ActorRef) extends Actor {

    import Tcp._
    import context.system

    IO(Tcp) ! Connect(new InetSocketAddress("www.virtual.ftnz.net", 27017))

    val log = Logging(context.system, this)

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

