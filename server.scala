
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

class Server extends Actor {

    import Tcp._
    import context.system

    val log = Logging(context.system, this)

    IO(Tcp) ! Bind(self, new InetSocketAddress("localhost", 27000))

    def receive = {
        case b @ Bound(localAddress) => log.info(s"bound to $localAddress")

        case CommandFailed(_: Bind) => context stop self

        case c @ Connected(remote, local) =>
            val connection = sender()
            val handler = context.actorOf(IncomingConnectionActor.props(connection))
            connection ! Register(handler)
    }
}

object IncomingConnectionActor {
    def props(listener:ActorRef) = Props(classOf[IncomingConnectionActor], listener)
}

case class Message(data:ByteString) {
    
}

class IncomingConnectionActor(connection:ActorRef) extends Actor {
    import Tcp._
    val client = context.actorOf(Client.props(self))

    def receive = {
        case Received(data) => {
            println("> " + Message(data))
            client ! data
        }
        case PeerClosed     => context stop self
        case data: ByteString => {
            println("< " + Message(data))
            connection ! Write(data)
        }
    }
}

object Client {
    def props(listener:ActorRef) = Props(classOf[Client], listener)
}

class Client(listener: ActorRef) extends Actor {

    import Tcp._
    import context.system

    IO(Tcp) ! Connect(new InetSocketAddress("localhost", 27017))

    def receive = {
        case CommandFailed(_: Connect) =>
              listener ! "connect failed"
              context stop self
        case c @ Connected(remote, local) =>
            listener ! c
            val connection = sender()
            connection ! Register(self)
            context become {
                case data: ByteString =>
                    connection ! Write(data)
                case CommandFailed(w: Write) =>
                // O/S buffer was full
                    listener ! "write failed"
                case Received(data) =>
                    listener ! data
                case "close" =>
                    connection ! Close
                case _: ConnectionClosed =>
                    listener ! "connection closed"
                    context stop self
            }
    }
}

