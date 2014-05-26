/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy.mongo

import org.scalatest._

import com.mongodb.casbah.Imports._
import org.zoy.kali.extropy._

class MessageParserSpec extends FlatSpec with Matchers {
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
        val op1 = OpReply(123, 12345678, 100, 2, Stream(MongoDBObject("a" -> 2), MongoDBObject("b" -> 12)))
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpReply])
        val op2:OpReply = IncomingMessage(bin).op.asInstanceOf[OpReply]
        op2.responseFlags should be(op1.responseFlags)
        op2.cursorID should be(op1.cursorID)
        op2.startingFrom should be(op1.startingFrom)
        op2.numberReturned should be(op1.numberReturned)
        op2.documents.size should be(op1.documents.size)
        op1.documents.zip(op2.documents).foreach( pair => pair._2 should be (pair._1) )
    }

    it should "pass OpMsg" in {
        val op1 = OpMsg("some message")
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpMsg])
        val op2:OpMsg = IncomingMessage(bin).op.asInstanceOf[OpMsg]
        op2.message should be(op1.message)
    }

    it should "pass OpUpdate" in {
        val op1 = OpUpdate(0, "some.collection", 12, MongoDBObject("a" -> 2), MongoDBObject("b" -> 12))
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpUpdate])
        val op2:OpUpdate = IncomingMessage(bin).op.asInstanceOf[OpUpdate]
        op2.zero should be(0)
        op2.fullCollectionName should be("some.collection")
        op2.flags should be(12)
        op2.selector should be(MongoDBObject("a" -> 2))
        op2.update should be(MongoDBObject("b" -> 12))
    }

    it should "pass OpInsert" in {
        val op1 = OpInsert(12, "some.collection", Stream(MongoDBObject("a" -> 2), MongoDBObject("b" -> 12)))
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpInsert])
        val op2:OpInsert = IncomingMessage(bin).op.asInstanceOf[OpInsert]
        op2.flags should be(12)
        op2.fullCollectionName should be("some.collection")
        op2.documents.toSeq should be( Seq(MongoDBObject("a" -> 2), MongoDBObject("b" -> 12)) )
    }

    it should "pass OpQuery without returnFieldsSelector" in {
        val op1 = OpQuery(12, "some.collection", 12, 42, MongoDBObject("a" -> 2), None)
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpQuery])
        val op2:OpQuery = IncomingMessage(bin).op.asInstanceOf[OpQuery]
        op2.flags should be(12)
        op2.fullCollectionName should be("some.collection")
        op2.numberToSkip should be(12)
        op2.numberToReturn should be(42)
        op2.query should be(MongoDBObject("a" -> 2))
        op2.returnFieldsSelector should be('empty)
    }

    it should "pass OpQuery with returnFieldsSelector" in {
        val op1 = OpQuery(12, "some.collection", 12, 42, MongoDBObject("a" -> 2), Some(MongoDBObject("b" -> 42)))
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpQuery])
        val op2:OpQuery = IncomingMessage(bin).op.asInstanceOf[OpQuery]
        op2.flags should be(12)
        op2.fullCollectionName should be("some.collection")
        op2.numberToSkip should be(12)
        op2.numberToReturn should be(42)
        op2.query should be(MongoDBObject("a" -> 2))
        op2.returnFieldsSelector should not be('empty)
        op2.returnFieldsSelector.get should be(MongoDBObject("b" -> 42))
    }

    it should "pass OpGetMore" in {
        val op1 = OpGetMore(0, "some.collection", 12, 42L)
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpGetMore])
        val op2:OpGetMore = IncomingMessage(bin).op.asInstanceOf[OpGetMore]
        op2.zero should be(0)
        op2.fullCollectionName should be("some.collection")
        op2.numberToReturn should be(12)
        op2.cursorID should be(42)
    }

    it should "pass OpDelete" in {
        val op1 = OpDelete(0, "some.collection", 12, MongoDBObject("a" -> 12))
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpDelete])
        val op2:OpDelete = IncomingMessage(bin).op.asInstanceOf[OpDelete]
        op2.zero should be(0)
    }

    it should "pass OpKillCursors" in {
        val op1 = OpKillCursors(0, 3, Stream(2L, 12L, 42L))
        val bin = CraftedMessage(1234, 5678, op1).binary
        IncomingMessage(bin).op.getClass should be (classOf[OpKillCursors])
        val op2:OpKillCursors = IncomingMessage(bin).op.asInstanceOf[OpKillCursors]
        op2.zero should be(0)
        op2.numberOfCursorIDs should be(3)
        op2.cursorIDs should be( Stream(2L, 12L, 42L) )
    }

    behavior of "Mongo message change detector"

    it should "identify change in a op insert" in {
        OpInsert(12, "foo.bar", Stream(MongoDBObject("a" -> 12))).asChanges should be(
            Iterable(InsertChange("foo.bar", MongoDBObject("a" -> 12)))
        )
    }

    it should "identify change in a op delete" in {
        OpDelete(0, "foo.bar", 12, MongoDBObject("a" -> 12)).asChanges should be(
            Iterable(DeleteChange("foo.bar", MongoDBObject("a" -> 12)))
        )
    }

    it should "identify change in both kinds of op update" in {
        OpUpdate(0, "foo.bar", 12, MongoDBObject("a" -> 12), MongoDBObject("b" -> 42)).asChanges should be(
            Iterable(FullBodyUpdateChange("foo.bar", MongoDBObject("a" -> 12), MongoDBObject("b" -> 42)))
        )
        OpUpdate(0, "foo.bar", 12, MongoDBObject("a" -> 12), MongoDBObject("$set" -> MongoDBObject("b" -> 42))).asChanges should be(
            Iterable(ModifiersUpdateChange("foo.bar", MongoDBObject("a" -> 12), MongoDBObject("$set" -> MongoDBObject("b" -> 42))))
        )
    }

    it should "identify update in findAndModify with full body" in {
        OpQuery(12, "foo.$cmd", 0, 0, MongoDBObject(
            "findAndModify" -> "bar",
            "query" -> MongoDBObject("a" -> 12),
            "update" -> MongoDBObject("b" -> 42)), None).asChanges should be(
            Iterable(FullBodyUpdateChange("foo.bar", MongoDBObject("a" -> 12), MongoDBObject("b" -> 42)))
        )
    }

    it should "identify update in findAndModify with modifiers" in {
        OpQuery(12, "foo.$cmd", 0, 0, MongoDBObject(
            "findAndModify" -> "bar",
            "query" -> MongoDBObject("a" -> 12),
            "update" -> MongoDBObject("$set" -> MongoDBObject("b" -> 42))), None).asChanges should be(
            Iterable(ModifiersUpdateChange("foo.bar", MongoDBObject("a" -> 12), MongoDBObject("$set" -> MongoDBObject("b" -> 42))))
        )
    }

    it should "identify delete in findAndModify with remove=true" in {
        OpQuery(12, "foo.$cmd", 0, 0, MongoDBObject(
            "findAndModify" -> "bar",
            "query" -> MongoDBObject("a" -> 12),
            "remove" -> true), None).asChanges should be (
                Iterable(DeleteChange("foo.bar", MongoDBObject("a" -> 12)))
        )
    }
}
