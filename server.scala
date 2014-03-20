
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

case class ParsedMessage(data:ByteString) {
    implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

    object OpCode extends Enumeration {
        type OpCode = Value
        val OP_REPLY = Value(1)
        val OP_MSG = Value(1000)
        val OP_UPDATE = Value(2001)
        val OP_INSERT = Value(2002)
        val RESERVED = Value(2003)
        val OP_QUERY = Value(2004)
        val OP_GET_MORE = Value(2005)
        val OP_DELETE = Value(2006)
        val OP_KILL_CURSORS = Value(2007)
    }

    abstract sealed class Op {}

    def readCString(iterator:ByteIterator):String = {
        val cstring = iterator.clone.takeWhile( _ != 0).toArray
        val result = new String(cstring, "UTF-8")
        iterator.drop(cstring.length)
        iterator.getByte // 0-terminated
        result
    }

    def readBSONObject(iterator:ByteIterator):BSONObject = {
        val size = iterator.clone.getInt
        val result = bsonDecoder.readObject(iterator.clone.take(size).toArray)
        iterator.drop(size)
        result
    }

    val bsonDecoder = new org.bson.LazyBSONDecoder

/*
struct OP_QUERY {
    MsgHeader header;                 // standard message header
    int32     flags;                  // bit vector of query options.  See below for details.
    cstring   fullCollectionName ;    // "dbname.collectionname"
    int32     numberToSkip;           // number of documents to skip
    int32     numberToReturn;         // number of documents to return
                                      //  in the first OP_REPLY batch
    document  query;                  // query object.  See below for details.
  [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields
                                      //  to return.  See below for details.
}
*/
    case class OpQuery( flags:Int, fullCollectionName:String, numberToSkip:Int, numberToReturn:Int,
                        query:BSONObject) extends Op
    object OpQuery {
        def parse(data:ByteString):OpQuery = {
            val it = data.iterator
            OpQuery(it.getInt,
                    readCString(it),
                    it.getInt, it.getInt,
                    readBSONObject(it))
        }
    }

/*
struct OP_REPLY {
    MsgHeader header;         // standard message header
    int32     responseFlags;  // bit vector - see details below
    int64     cursorID;       // cursor id if client needs to do get more's
    int32     startingFrom;   // where in the cursor this reply is starting
    int32     numberReturned; // number of documents in the reply
    document* documents;      // documents
}
*/
    case class OpReply( responseFlags:Int, cursorID:Long, startingFrom:Int, numberReturned:Int,
                        documents:Stream[BSONObject]) extends Op
    object OpReply {
        def parse(data:ByteString):OpReply = {
            val it = data.iterator
            val (flags, cursorID, startingFrom, numberReturned)
                = (it.getInt, it.getLong, it.getInt, it.getInt)
            OpReply(flags, cursorID, startingFrom, numberReturned,
                Stream.fill(numberReturned)( readBSONObject(it) )
            )
        }
    }

/*
struct OP_GET_MORE {
    MsgHeader header;             // standard message header
    int32     ZERO;               // 0 - reserved for future use
    cstring   fullCollectionName; // "dbname.collectionname"
    int32     numberToReturn;     // number of documents to return
    int64     cursorID;           // cursorID from the OP_REPLY
}
*/
    case class OpGetMore(zero:Int, fullCollectionName:String, numberReturned:Int, cursorID:Long) extends Op
    object OpGetMore {
        def parse(data:ByteString):OpGetMore = {
            val it = data.iterator
            OpGetMore(it.getInt, readCString(it), it.getInt, it.getLong)
        }
    }

    case class OpUnknown extends Op {}

    case class Header(messageLength:Int, requestId:Int, responseTo:Int, opCode:OpCode.Value)

    lazy val header = {
        val it = data.iterator
        Header(it.getInt, it.getInt, it.getInt, OpCode(it.getInt))
    }

    lazy val op = header.opCode match {
        case OpCode.OP_QUERY => OpQuery.parse(data.drop(16))
        case OpCode.OP_REPLY => OpReply.parse(data.drop(16))
        case OpCode.OP_GET_MORE => OpGetMore.parse(data.drop(16))
        case _ => OpUnknown
    }
    override def toString() = s"$header: $op"
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
            println("> " + ParsedMessage(data))
            incomingBuffer ++= data
            splitAndSend

        case PeerClosed     => context stop self
        case data:ByteString =>
            println("< " + ParsedMessage(data))
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

