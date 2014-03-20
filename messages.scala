package org.zoy.kali.extropy

import akka.util.{ ByteString, ByteIterator }
import org.bson.BSONObject

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


