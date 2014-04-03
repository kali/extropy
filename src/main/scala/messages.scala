package org.zoy.kali.extropy.mongo

import akka.util.{ ByteString, ByteStringBuilder, ByteIterator }
import org.bson.BSONObject

object MessageParser {
    implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN

    def readCString(iterator:ByteIterator):String = {
        val cstring = iterator.clone.takeWhile( _ != 0).toArray
        val result = new String(cstring, "UTF-8")
        iterator.drop(cstring.length)
        iterator.getByte // 0-terminated
        result
    }

    val bsonDecoder = new org.bson.BasicBSONDecoder
    val bsonEncoder = new org.bson.BasicBSONEncoder
    def readBSONObject(iterator:ByteIterator):BSONObject = {
        val size = iterator.clone.getInt
        val result = bsonDecoder.readObject(iterator.clone.take(size).toArray)
        iterator.drop(size)
        result
    }
}
import MessageParser.{ _byteOrder, readCString, readBSONObject, bsonEncoder, bsonDecoder }

sealed abstract class Message {
    def binary:ByteString
    def header:MsgHeader
    def op:Op
    def isWriteOp:Boolean
}

case class IncomingMessage(val binary:ByteString) extends Message {
    val header = MsgHeader.parse(binary.iterator)
    val isWriteOp = List(2001,2002,2006).contains(header.opCode)
    lazy val op = header.opCode match {
        case    1 => OpReply.parse(binary.iterator.drop(16))
        case 1000 => OpMsg.parse(binary.iterator.drop(16))
        case 2001 => OpUpdate.parse(binary.iterator.drop(16))
        case 2002 => OpInsert.parse(binary.iterator.drop(16))
        case 2003 => OpReserved()
        case 2004 => OpQuery.parse(binary.iterator.drop(16))
        case 2005 => OpGetMore.parse(binary.iterator.drop(16))
        case 2006 => OpDelete.parse(binary.iterator.drop(16))
        case 2007 => OpKillCursors.parse(binary.iterator.drop(16))
    }
}

case class CraftedMessage(requestId:Int, responseTo:Int, op:Op) extends Message {
    lazy val header = MsgHeader(16 + op.binary.size, requestId, responseTo, op.opcode)
    lazy val binary = header.binary ++ op.binary
    val isWriteOp = op.isWriteOp
}

case class MsgHeader(messageLength:Int, requestId:Int, responseTo:Int, opCode:Int) {
    def binary:ByteString =
        new ByteStringBuilder() .putInt(messageLength).putInt(requestId)
                                .putInt(responseTo).putInt(opCode).result
}
object MsgHeader {
    def parse(it:ByteIterator) = MsgHeader(it.getInt, it.getInt, it.getInt, it.getInt)
}
abstract sealed class Op {
    def binary:ByteString
    def opcode:Int
    def isWriteOp = List(2001, 2002, 2006).contains(opcode)
}
/*
def parse(data:ByteString):IncomingMessage = {
    val it = data.iterator
    val header = MsgHeader.parse(it)
    IncomingMessage(header, header.opCode match {
        case    1 => OpReply.parse(it)
        case 1000 => OpMsg.parse(it)
        case 2001 => OpUpdate.parse(it)
        case 2002 => OpInsert.parse(it)
        case 2003 => OpReserved()
        case 2004 => OpQuery.parse(it)
        case 2005 => OpGetMore.parse(it)
        case 2006 => OpDelete.parse(it)
        case 2007 => OpKillCursors.parse(it)
    })
}
*/
/*
struct OP_UPDATE {
    MsgHeader header;             // standard message header
    int32     ZERO;               // 0 - reserved for future use
    cstring   fullCollectionName; // "dbname.collectionname"
    int32     flags;              // bit vector. see below
    document  selector;           // the query to select the document
    document  update;             // specification of the update to perform
}
*/
case class OpUpdate( zero:Int, fullCollectionName:String, flags:Int,
                     selector:BSONObject, update:BSONObject) extends Op {
    def opcode = 2001
    def binary:ByteString =
        new ByteStringBuilder()
                                .putInt(zero)
                                .putBytes(fullCollectionName.getBytes("UTF-8")).putByte(0)
                                .putInt(flags)
                                .putBytes(bsonEncoder.encode(selector))
                                .putBytes(bsonEncoder.encode(update))
                                .result
}
object OpUpdate {
    def parse(it:ByteIterator):OpUpdate = {
        OpUpdate(it.getInt, readCString(it), it.getInt,
                readBSONObject(it), readBSONObject(it))
    }
}

/*
struct OP_INSERT {
    MsgHeader header;             // standard message header
    int32     flags;              // bit vector - see below
    cstring   fullCollectionName; // "dbname.collectionname"
    document* documents;          // one or more documents to insert into the collection
}
*/
case class OpInsert(flags:Int, fullCollectionName:String,
                    documents:Stream[BSONObject]) extends Op {
    def opcode = 2002
    def binary:ByteString = {
        val bsb = new ByteStringBuilder()
                                .putInt(flags)
                                .putBytes(fullCollectionName.getBytes("UTF-8")).putByte(0)
        documents.foreach( doc => bsb.putBytes(bsonEncoder.encode(doc)) )
        bsb.result
    }
}
object OpInsert {
    def parse(it:ByteIterator):OpInsert = {
        OpInsert(it.getInt, readCString(it),
            new Iterator[BSONObject] {
                override def hasNext = it.hasNext
                override def next = readBSONObject(it)
            }.toStream
        )
    }
}
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
                    query:BSONObject, returnFieldsSelector:Option[BSONObject]) extends Op {
    def opcode = 2004
    def binary:ByteString = {
        val bsb = new ByteStringBuilder()
                                .putInt(flags)
                                .putBytes(fullCollectionName.getBytes("UTF-8")).putByte(0)
                                .putInt(numberToSkip)
                                .putInt(numberToReturn)
                                .putBytes(bsonEncoder.encode(query))
        returnFieldsSelector.foreach( sel => bsb.putBytes(bsonEncoder.encode(sel)) )
        bsb.result
    }
}
object OpQuery {
    def parse(it:ByteIterator):OpQuery = {
        OpQuery(it.getInt,
                readCString(it),
                it.getInt, it.getInt,
                readBSONObject(it),
                if(it.hasNext) Some(readBSONObject(it)) else None)
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
case class OpGetMore(   zero:Int, fullCollectionName:String,
                        numberToReturn:Int, cursorID:Long) extends Op {
    def opcode = 2005
    def binary:ByteString = {
        new ByteStringBuilder()
                                .putInt(zero)
                                .putBytes(fullCollectionName.getBytes("UTF-8")).putByte(0)
                                .putInt(numberToReturn)
                                .putLong(cursorID)
                                .result
    }
}
object OpGetMore {
    def parse(it:ByteIterator):OpGetMore = {
        OpGetMore(it.getInt, readCString(it), it.getInt, it.getLong)
    }
}

/*
struct OP_DELETE {
    MsgHeader header;             // standard message header
    int32     ZERO;               // 0 - reserved for future use
    cstring   fullCollectionName; // "dbname.collectionname"
    int32     flags;              // bit vector - see below for details.
    document  selector;           // query object.  See below for details.
}
*/
case class OpDelete(zero:Int, fullCollectionName:String, flags:Int, selector:BSONObject) extends Op {
    def opcode = 2006
    def binary:ByteString = {
        new ByteStringBuilder()
                                .putInt(zero)
                                .putBytes(fullCollectionName.getBytes("UTF-8")).putByte(0)
                                .putInt(flags)
                                .putBytes(bsonEncoder.encode(selector))
                                .result
    }
}
object OpDelete {
    def parse(it:ByteIterator):OpDelete = {
        OpDelete(it.getInt, readCString(it), it.getInt, readBSONObject(it))
    }
}

/*
struct OP_KILL_CURSORS {
    MsgHeader header;            // standard message header
    int32     ZERO;              // 0 - reserved for future use
    int32     numberOfCursorIDs; // number of cursorIDs in message
    int64*    cursorIDs;         // sequence of cursorIDs to close
}
*/
case class OpKillCursors(zero:Int, numberOfCursorIDs:Int, cursorIDs:Stream[Long]) extends Op {
    def opcode = 2007
    def binary:ByteString = {
        val bsb = new ByteStringBuilder()
                                .putInt(zero)
                                .putInt(numberOfCursorIDs)
        cursorIDs.foreach( c => bsb.putLong(c) )
        bsb.result
    }
}
object OpKillCursors {
    def parse(it:ByteIterator):OpKillCursors = {
        val (zero, numberOfCursorIDs) = (it.getInt, it.getInt)
        OpKillCursors(zero, numberOfCursorIDs, Stream.fill(numberOfCursorIDs)(it.getLong))
    }
}

/* struct OP_MSG { // DEPRECATED
    MsgHeader header;  // standard message header
    cstring   message; // message for the database
}
*/
case class OpMsg(message:String) extends Op {
    def opcode = 1000
    def binary:ByteString = {
        new ByteStringBuilder()
                                .putBytes(message.getBytes("UTF-8")).putByte(0)
                                .result
    }
}
object OpMsg {
    def parse(it:ByteIterator):OpMsg = OpMsg(readCString(it))
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
                    documents:Stream[BSONObject]) extends Op {
    def opcode = 1
    def binary:ByteString = {
        val bsb = new ByteStringBuilder()
                                .putInt(responseFlags)
                                .putLong(cursorID)
                                .putInt(startingFrom)
                                .putInt(numberReturned)
        documents.foreach( doc => bsb.putBytes(bsonEncoder.encode(doc)) )
        bsb.result
    }
}
object OpReply {
    def parse(it:ByteIterator):OpReply = {
        val (flags, cursorID, startingFrom, numberReturned)
            = (it.getInt, it.getLong, it.getInt, it.getInt)
        OpReply(flags, cursorID, startingFrom, numberReturned,
            Stream.fill(numberReturned)( readBSONObject(it) )
        )
    }
}

/* OP_RESERVED */
case class OpReserved() extends Op {
    def opcode = 2003
    def binary:ByteString = ByteString.empty
}
