package org.zoy.kali.extropy

import akka.util.{ ByteString, ByteIterator }
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

    val bsonDecoder = new org.bson.LazyBSONDecoder
    def readBSONObject(iterator:ByteIterator):BSONObject = {
        val size = iterator.clone.getInt
        val result = bsonDecoder.readObject(iterator.clone.take(size).toArray)
        iterator.drop(size)
        result
    }

    case class MsgHeader(messageLength:Int, requestId:Int, responseTo:Int, opCode:Int)
    abstract sealed class Op {}

    def parse(data:ByteString):Op = {
        val it = data.iterator
        val header = MsgHeader(it.getInt, it.getInt, it.getInt, it.getInt)
        header.opCode match {
            case    1 => OpReply.parse(header, it)
            case 1000 => OpMsg.parse(header, it)
            case 2001 => OpUpdate.parse(header, it)
            case 2002 => OpInsert.parse(header, it)
            case 2003 => OpReserved(header)
            case 2004 => OpQuery.parse(header, it)
            case 2005 => OpGetMore.parse(header, it)
            case 2006 => OpDelete.parse(header, it)
            case 2007 => OpKillCursors.parse(header, it)
        }
    }

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
    case class OpUpdate( header:MsgHeader, zero:Int, fullCollectionName:String, flags:Int,
                         selector:BSONObject, update:BSONObject) extends Op
    object OpUpdate {
        def parse(header:MsgHeader, it:ByteIterator):OpUpdate = {
            OpUpdate(header, it.getInt, readCString(it), it.getInt,
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
    case class OpInsert(header:MsgHeader, flags:Int, fullCollectionName:String,
                        documents:Stream[BSONObject]) extends Op
    object OpInsert {
        def parse(header:MsgHeader, it:ByteIterator):OpInsert = {
            OpInsert(header, it.getInt, readCString(it),
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
    case class OpQuery( header:MsgHeader, flags:Int, fullCollectionName:String, numberToSkip:Int, numberToReturn:Int,
                        query:BSONObject, returnFieldsSelector:Option[BSONObject]) extends Op
    object OpQuery {
        def parse(header:MsgHeader, it:ByteIterator):OpQuery = {
            OpQuery(header, it.getInt,
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
    case class OpGetMore(header:MsgHeader, zero:Int, fullCollectionName:String, numberReturned:Int, cursorID:Long) extends Op
    object OpGetMore {
        def parse(header:MsgHeader, it:ByteIterator):OpGetMore = {
            OpGetMore(header, it.getInt, readCString(it), it.getInt, it.getLong)
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
    case class OpDelete(header:MsgHeader, zero:Int, fullCollectionName:String, flags:Int, selector:BSONObject) extends Op
    object OpDelete {
        def parse(header:MsgHeader, it:ByteIterator):OpDelete = {
            OpDelete(header, it.getInt, readCString(it), it.getInt, readBSONObject(it))
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
    case class OpKillCursors(header:MsgHeader, zero:Int, numberOfCursorIDs:Int, cursorIDs:Stream[Long]) extends Op
    object OpKillCursors {
        def parse(header:MsgHeader, it:ByteIterator):OpKillCursors = {
            val (zero, numberOfCursorIDs) = (it.getInt, it.getInt)
            OpKillCursors(header, zero, numberOfCursorIDs, Stream.fill(numberOfCursorIDs)(it.getLong))
        }
    }

    /* struct OP_MSG { // DEPRECATED
        MsgHeader header;  // standard message header
        cstring   message; // message for the database
    }
    */
    case class OpMsg(header:MsgHeader, message:String) extends Op
    object OpMsg {
        def parse(header:MsgHeader, it:ByteIterator):OpMsg = OpMsg(header, readCString(it))
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
    case class OpReply( header:MsgHeader, responseFlags:Int, cursorID:Long, startingFrom:Int, numberReturned:Int,
                        documents:Stream[BSONObject]) extends Op
    object OpReply {
        def parse(header:MsgHeader, it:ByteIterator):OpReply = {
            val (flags, cursorID, startingFrom, numberReturned)
                = (it.getInt, it.getLong, it.getInt, it.getInt)
            OpReply(header, flags, cursorID, startingFrom, numberReturned,
                Stream.fill(numberReturned)( readBSONObject(it) )
            )
        }
    }

    /* OP_RESERVED */
    case class OpReserved(header:MsgHeader) extends Op
}


