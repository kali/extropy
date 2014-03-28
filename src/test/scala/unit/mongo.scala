package org.zoy.kali.extropy.mongo

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import com.mongodb.casbah.Imports._

class MongoSpec extends FlatSpec with ShouldMatchers {
    implicit val _byteOrder = java.nio.ByteOrder.LITTLE_ENDIAN
    behavior of "Mongo message codec"

    it should "pass requestId, responseTo and size" in {
        val pre = CraftedMessage(1234, 5678, OpMsg("blah blah"))
        val bin = pre.binary
        bin.iterator.getInt should be(bin.size)
        IncomingMessage(bin).header.requestId should be(pre.requestId)
        IncomingMessage(bin).header.responseTo should be(pre.responseTo)
    }

    it should "pass OpReply" in {
        val op1 = OpReply(123, 12345678, 100, 2, Array(MongoDBObject("a" -> 2), MongoDBObject("b" -> 12)).toStream)
        val pre = CraftedMessage(1234, 5678, op1)
        val bin = pre.binary
        IncomingMessage(bin).op.getClass should be (classOf[OpReply])
        val op2:OpReply = IncomingMessage(bin).op.asInstanceOf[OpReply]
        op2.responseFlags should be(op1.responseFlags)
        op2.cursorID should be(op1.cursorID)
        op2.startingFrom should be(op1.startingFrom)
        op2.numberReturned should be(op1.numberReturned)
        op2.documents.size should be(op1.documents.size)
        op1.documents.zip(op2.documents).foreach( pair => pair._2 should be (pair._1) )
    }
}
