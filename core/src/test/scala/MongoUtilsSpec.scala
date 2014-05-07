package org.zoy.kali.extropy.mongoutils

import org.scalatest._
import org.scalatest.matchers.ShouldMatchers

import org.zoy.kali.extropy.MongodbTemporary

import com.mongodb.casbah.Imports._

import scala.concurrent.duration._

import org.bson.BSONObject

import BSONObjectConversions._

class MongoUtilsSpec extends FlatSpec with ShouldMatchers {
    behavior of "MongoUtils"

    val (x,y,z,t) = ("x","y","z","t")
    implicit def m[A <: String, B] (elems: (A, B)*) : DBObject = MongoDBObject(elems:_*)
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

class MongoLockingPoolSpec extends FlatSpec with ShouldMatchers with BeforeAndAfterAll with BeforeAndAfterEach
    with MongodbTemporary {
    behavior of "A mongo locking pool"

    var collection:MongoCollection = null
    var mlp:MongoLockingPool = null
    override def beforeEach {
        super.beforeEach
        collection = mongoBackendClient("test")("mongo_locking_pool_test")
        collection.drop
        mlp = MongoLockingPool(collection, defaultTimeout=100 milliseconds)
    }

    implicit val _lockerId:LockerIdentity = LockerIdentity("me")

    it should "bless records iif they are not blessed already" in {
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

    it should "allow blessing at insertion" in {
        collection.insert(MongoDBObject("_id" -> "foo") ++ mlp.blessing)
        var doc = collection.find(MongoDBObject("_id" -> "foo")).next.toMap
        doc should contain key(mlp.subfield)
        doc.get(mlp.subfield) shouldBe a [BSONObject]
        doc.get(mlp.subfield).asInstanceOf[BSONObject].toMap should contain key("lb")

        collection.insert(mlp.blessed(MongoDBObject("_id" -> "bar")))
        doc = collection.find(MongoDBObject("_id" -> "bar")).next.toMap
        doc should contain key(mlp.subfield)
    }

    it should "allow locked insertion" in {
        mlp.insertLocked(MongoDBObject("_id" -> "foo"))
        var doc = collection.find(MongoDBObject("_id" -> "foo")).next.toMap
        doc should contain key(mlp.subfield)
        doc.get(mlp.subfield) shouldBe a [BSONObject]
        doc.get(mlp.subfield).asInstanceOf[BSONObject].toMap should contain key("lb")

        mlp.lockOne() should be('empty)
    }

    it should "offer exactly two locks when there are two blessed objects" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")), mlp.blessed(MongoDBObject("_id" -> "bar")))
        mlp.lockOne() should not be('empty)
        mlp.lockOne() should not be('empty)
        mlp.lockOne() should be('empty)
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "baz")))
        mlp.lockOne() should not be('empty)
    }

    it should "lock and release" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        mlp.lockOne() should be ('empty)
        mlp.release(lock)
        mlp.lockOne() should not be('empty)
    }

    it should "prevent release by another locker" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        an [Exception] should be thrownBy {
            mlp.release(lock)(LockerIdentity("not me"))
        }
    }

    it should "prevent late release" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        Thread.sleep( mlp.defaultTimeout.toMillis * 2 )
        val thrown = the [Exception] thrownBy mlp.release(lock)
        thrown.getMessage should include("has expired")
    }

    it should "lock and delete on unlock" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        mlp.lockOne() should be ('empty)
        mlp.release(lock, delete=true)
        collection.size should be(0)
    }

    it should "ignore obsolete locks when locking" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        mlp.lockOne() should not be ('empty)
        mlp.lockOne() should be ('empty)
        Thread.sleep( mlp.defaultTimeout.toMillis * 2)
        mlp.lockOne() should not be ('empty)
    }

    it should "relock locks" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        mlp.lockOne() should be ('empty)
        (0 to 20).foreach { i =>
            Thread.sleep( mlp.defaultTimeout.toMillis / 10 )
            mlp.relock(lock)
        }
        mlp.lockOne() should be ('empty)
    }

    it should "prevent relock by another locker" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        val thrown = the [Exception] thrownBy mlp.relock(lock)(LockerIdentity("not me"))
        thrown.getMessage should include("not mine")
    }

    it should "prevent relock after the timeout" in {
        collection.insert(mlp.blessed(MongoDBObject("_id" -> "foo")))
        val lock = mlp.lockOne().get
        Thread.sleep( mlp.defaultTimeout.toMillis * 2 )
        an [Exception] should be thrownBy {
            mlp.relock(lock)
        }
    }
}
