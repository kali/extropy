/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy.mongoutils

import org.scalatest._

import org.zoy.kali.extropy.MongodbTemporary
import de.flapdoodle.embed.mongo.distribution.{ IFeatureAwareVersion, Version }

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

import org.bson.BSONObject

import BSONObjectConversions._


class MongoUtilsSpec extends FlatSpec with Matchers {
    behavior of "MongoUtils"

    val (x,y,z,t) = ("x","y","z","t")
    def m[A <: String, B] (elems: (A, B)*) : DBObject = MongoDBObject(elems:_*)
    import MongoUtils.recursiveMerge
    it should "merge first level of DBObject" in {
        recursiveMerge(m(x->m(x->x))) should be (m(x->m(x->x)))
        recursiveMerge(m(x->m(x->x)),m(y->m(y->y)),m(z->m(z->z))) should be (m(x->m(x->x),y->m(y->y),z->m(z->z)))
        recursiveMerge(m(x->m(x->x)),m(x->m(y->y)),m(x->m(z->z))) should be (m(x->m(x->x,y->y,z->z)))
        recursiveMerge(m(x->m(x->x)),m(x->m(x->y))) should be (m(x->m(x->y)))
    }

    it should "merge recursively" in {
        recursiveMerge( m(t->m(x->m(x->x))),
                        m(t->m(x->m(y->y))),
                        m(t->m(x->m(z->z))) ) should be( m(t->m(x->m(x->x,y->y,z->z))) )
    }
}

abstract class MongoLockingPoolSpec extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach
    with MongodbTemporary {
    behavior of "A mongo locking pool"

    def withLockingPool(testCode:(MongoCollection,MongoLockingPool)=>Any) {
            val collection = mongoBackendClient("test")(s"mongo_locking_pool_test-${System.currentTimeMillis}")
            val mlp = MongoLockingPool(collection, defaultTimeout=500.milliseconds)
            testCode(collection,mlp)
            collection.drop
    }

    implicit val _lockerId:LockerIdentity = LockerIdentity("me")

    it should "bless records iif they are not blessed already" in withLockingPool { (collection,mlp) =>
        collection.save(MongoDBObject("_id" -> "foo", "bar" -> "baz"))
        mlp.bless("foo")
        var doc = collection.find(MongoDBObject("_id" -> "foo")).next.toMap
        doc should contain key(mlp.subfield)
        doc.get(mlp.subfield) shouldBe a [BSONObject]
        doc.get(mlp.subfield).asInstanceOf[BSONObject].toMap should contain key("lb")

        collection.update(MongoDBObject("_id" -> "foo"),
            MongoDBObject("$set" -> MongoDBObject(mlp.subfield->MongoDBObject.empty)))
        doc = collection.find(MongoDBObject("_id" -> "foo")).next.toMap
        doc.get(mlp.subfield).asInstanceOf[BSONObject].toMap should not contain key("lb")

        mlp.bless("foo")
        doc = collection.find(MongoDBObject("_id" -> "foo")).next.toMap
        doc.get(mlp.subfield).asInstanceOf[BSONObject].toMap should not contain key("lb")
    }

    it should "allow blessing at insertion" in withLockingPool { (collection,mlp) =>
        collection.insert(MongoDBObject("_id" -> "foo") ++ mlp.blessing)
        var doc = collection.find(MongoDBObject("_id" -> "foo")).next.toMap
        doc should contain key(mlp.subfield)
        doc.get(mlp.subfield) shouldBe a [BSONObject]
        doc.get(mlp.subfield).asInstanceOf[BSONObject].toMap should contain key("lb")

        collection.insert(mlp.blessed(MongoDBObject("_id" -> "bar")))
        doc = collection.find(MongoDBObject("_id" -> "bar")).next.toMap
        doc should contain key(mlp.subfield)
    }

    it should "allow locked insertion" in withLockingPool { (collection,mlp) =>
        mlp.insertLocked(MongoDBObject("_id" -> "foo"))
        var doc = collection.find(MongoDBObject("_id" -> "foo")).next.toMap
        doc should contain key(mlp.subfield)
        doc.get(mlp.subfield) shouldBe a [BSONObject]
        doc.get(mlp.subfield).asInstanceOf[BSONObject].toMap should contain key("lb")

        mlp.lockOne() should be('empty)
    }

    it should "offer exactly two locks when there are two blessed objects" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")), mlp.blessed(MongoDBObject("_id" -> "bar")))
        mlp.lockOne() should not be('empty)
        mlp.lockOne() should not be('empty)
        mlp.lockOne() should be('empty)
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "baz")))
        mlp.lockOne() should not be('empty)
    }

    it should "lock and release" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        mlp.lockOne() should be ('empty)
        mlp.release(lock)
        mlp.lockOne() should not be('empty)
    }

    it should "prevent release by another locker" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        an [Exception] should be thrownBy {
            mlp.release(lock)(LockerIdentity("not me"))
        }
    }

    it should "prevent late release" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        Thread.sleep( mlp.defaultTimeout.toMillis * 2 )
        val thrown = the [Exception] thrownBy mlp.release(lock)
        thrown.getMessage should include("has expired")
    }

    it should "lock and delete on unlock" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        mlp.lockOne() should be ('empty)
        mlp.release(lock, delete=true)
        collection.size should be(0)
    }

    it should "ignore obsolete locks when locking" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        mlp.lockOne() should not be ('empty)
        mlp.lockOne() should be ('empty)
        Thread.sleep( mlp.defaultTimeout.toMillis * 2)
        mlp.lockOne() should not be ('empty)
    }

    it should "relock locks" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        mlp.lockOne() should be ('empty)
        (0 to 20).foreach { i =>
            Thread.sleep( mlp.defaultTimeout.toMillis / 10 )
            mlp.relock(lock)
        }
        mlp.lockOne() should be ('empty)
    }

    it should "prevent relock by another locker" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        val thrown = the [Exception] thrownBy mlp.relock(lock)(LockerIdentity("not me"))
        thrown.getMessage should include("not mine")
    }

    it should "prevent relock after the timeout" in withLockingPool { (collection,mlp) =>
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        Thread.sleep( mlp.defaultTimeout.toMillis * 2 )
        an [Exception] should be thrownBy {
            mlp.relock(lock)
        }
    }
}

class MongoLockingPoolSpec_Proto2_4 extends MongoLockingPoolSpec {
    override def mongoWantedVersion = Some(Version.Main.V2_4)
}

class MongoLockingPoolSpec_Proto2_6 extends MongoLockingPoolSpec {
    override def mongoWantedVersion = Some(Version.Main.V2_6)
}
