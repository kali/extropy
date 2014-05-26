/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy.mongoutils

import java.util.Date

import com.mongodb.casbah.Imports._
import scala.concurrent.duration._
import com.novus.salat.annotations._

import com.typesafe.scalalogging.slf4j.StrictLogging

import org.bson.BSONObject

object BSONObjectConversions {
    import scala.language.implicitConversions
    implicit def bsonObject2dbObject(bson:BSONObject):DBObject = bson match {
        case dbo:DBObject => dbo
        case bson => new BasicDBObject(bson.toMap)
    }
/*
    implicit def bsonObject2scalaMap(bson:BSONObject):scala.collection.mutable.Map[String,AnyRef] =
        scala.collection.convert.WrapAsScala.mapAsScalaMap[String,AnyRef](bson.toMap.asInstanceOf[java.util.Map[String,AnyRef]])
*/
    implicit def bsonObject2MongoDBObject(bson:BSONObject):MongoDBObject = new MongoDBObject(bson)
}

import BSONObjectConversions._

object MongoUtils {
    def recursiveMerge( docs:BSONObject* ):BSONObject =
        docs.foldLeft(MongoDBObject.empty) { (aggregate, increment) =>
            (aggregate.toSeq ++ increment.toSeq).groupBy(_._1).map { case (key, value) =>
                // groups are generated this way key1,[(key1,value1),(key1,value2)]
                val values = value.map( _._2 )
                val result = if(values.forall( _.isInstanceOf[BSONObject] ))
                    recursiveMerge(values.map( _.asInstanceOf[BSONObject]):_*)
                else
                    values.last
                (key, result)
            }.toMap
        }
}

case class LockerIdentity(id:AnyRef)
case class MongoLock(@Key("lu") until:Date, @Key("lb") locker:Option[AnyRef]) {
    def stillValid = System.currentTimeMillis < until.getTime
}
object MongoLock {
    val empty = MongoLock(new Date(0), None)
}

case class MongoLockingPool(
    collection:MongoCollection,
    defaultTimeout:FiniteDuration=1 second
) extends StrictLogging {

    import MongoUtils.recursiveMerge

    def subfield:String = "emlp"

    def blessing:BSONObject = MongoDBObject(subfield -> blessingData)
    def blessingData:BSONObject = MongoDBObject("lb" -> null, "lu" -> new Date(0))
    def bless(id:AnyRef) {
        collection.update(
            MongoDBObject("_id" -> id, subfield -> MongoDBObject("$exists" -> false)),
            MongoDBObject("$set" -> blessing))
    }
    def blessed(o:BSONObject) = recursiveMerge(o,blessing)

    def defaultLockingQueryCriteria:BSONObject = MongoDBObject.empty
    def defaultLockingSortCriteria:BSONObject = MongoDBObject.empty

    def lockUpdate(timeout:FiniteDuration)(implicit by:LockerIdentity):BSONObject =
        MongoDBObject("$set" -> MongoDBObject(
            s"$subfield.lb" -> by.id,
            s"$subfield.lu" -> new Date(timeout.fromNow.time.toMillis)
        ))

    def lockOne(selectorCriteria:BSONObject=null,sortCriteria:BSONObject=null,
                updater:BSONObject=null,timeout:FiniteDuration=defaultTimeout)
                (implicit by:LockerIdentity):Option[BSONObject] = {
        val result = collection.findAndModify(
            recursiveMerge(defaultLockingQueryCriteria,
                MongoDBObject( s"$subfield.lu" -> MongoDBObject("$lt" -> new Date()) ),
                Option(selectorCriteria).getOrElse(MongoDBObject.empty)),
            sort=if(sortCriteria != null) sortCriteria else defaultLockingSortCriteria,
            update = if(updater!=null) recursiveMerge(lockUpdate(timeout), updater) else lockUpdate(timeout)
        )
        logger.trace(s"lockOne $collection: $by locks $result")
        result
    }

    def cleanupOldLocks {
        collection.remove(MongoDBObject( s"$subfield.lu" -> MongoDBObject("$lt" -> new Date()) ))
    }

    def insertLocked(doc:BSONObject, timeout:FiniteDuration=defaultTimeout)(implicit by:LockerIdentity) {
        collection.insert(recursiveMerge(doc, MongoDBObject(subfield ->
            MongoDBObject("lb" -> by.id, "lu" -> new Date(timeout.fromNow.time.toMillis))
        )))
        logger.trace(s"insertLocked $collection: $by creates and locks $doc")
    }

    def ownedLockQueryCriteria(lock:BSONObject)(implicit by:LockerIdentity) = MongoDBObject(
        s"$subfield.lb" -> by.id, "_id" -> lock.get("_id"),
        s"$subfield.lu" -> MongoDBObject("$gt" -> new Date())
    )

    def release(lock:BSONObject, update:BSONObject=null, delete:Boolean=false)(implicit by:LockerIdentity) {
        val u = MongoDBObject("$set" -> MongoDBObject(s"$subfield.lb" -> null, s"$subfield.lu" -> new Date(0)))
        (if(delete)
            collection.findAndRemove(ownedLockQueryCriteria(lock))
        else if(update != null)
            collection.findAndModify(ownedLockQueryCriteria(lock), update=recursiveMerge(u, update))
        else
            collection.findAndModify(ownedLockQueryCriteria(lock), u)
        ).orElse(throw new IllegalStateException(s"failure to release $lock in $collection because: ${diagnoseFailure(lock)}"))
        logger.trace(s"release $collection: $by release ${lock._id} delete=$delete update=$update")
    }

    def relock(lock:BSONObject,timeout:FiniteDuration=defaultTimeout)(implicit by:LockerIdentity):BSONObject = {
        val result = collection.findAndModify(
            query= ownedLockQueryCriteria(lock),
            fields= MongoDBObject.empty,
            sort= MongoDBObject.empty,
            remove= false,
            update= lockUpdate(timeout),
            returnNew= true,
            upsert= false
        ).getOrElse(throw new IllegalStateException(s"failure to relock $lock in $collection because: ${diagnoseFailure(lock)}"))
        logger.trace(s"relock $collection: $by relock ${lock} [${ownedLockQueryCriteria(lock)}] for $defaultTimeout")
        result
    }

    def diagnoseFailure(lock:BSONObject)(implicit by:LockerIdentity):String =
        collection.findOne(MongoDBObject("_id" -> lock.get("_id"))) match {
            case None => s"no lock found"
            case Some(other) => other.getAs[BSONObject](subfield) match {
                case None => s"document found, but is unblessed"
                case Some(blessing) => {
                    val lu:Option[Date] = blessing.getAs[Date]("lu")
                    val lb:Option[AnyRef] = blessing.getAs[AnyRef]("lb")
                    if(lu.isEmpty)
                        s"lock record has no lu (locked until): $blessing"
                    else if(lb.isEmpty)
                        s"lock record has no lb (locked by): $blessing"
                    else if(lu.get.getTime < System.currentTimeMillis)
                        s"lock record has expired ${ System.currentTimeMillis - lu.get.getTime }ms ago: $blessing"
                    else if(lu.get != by.id)
                        s"lock is not mine (me:$by lock:$lb): $blessing"
                    else
                        s"no idea why: $blessing"
                }
            }
        }
}
