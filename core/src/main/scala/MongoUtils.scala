package org.zoy.kali.extropy.mongo

import java.util.Date

import com.mongodb.casbah.Imports._
import scala.concurrent.duration._

object MongoUtils {
    def recursiveMerge( docs:DBObject* ):DBObject =
        docs.foldLeft(MongoDBObject.empty) { (aggregate, increment) =>
            (aggregate.toSeq ++ increment.toSeq).groupBy(_._1).map { case (key, value) =>
                // groups are generated this way key1,[(key1,value1),(key1,value2)]
                val values = value.map( _._2 )
                val result = if(values.forall( _.isInstanceOf[DBObject] ))
                    recursiveMerge(values.map( _.asInstanceOf[DBObject]):_*)
                else
                    values.last
                (key, result)
            }.toMap
        }
}

object MongoLockingPool {
    case class LockerIdentity(id:AnyRef)
}

case class MongoLockingPool(
    collection:MongoCollection,
    defaultTimeout:FiniteDuration=1 second
) {

    import MongoLockingPool.LockerIdentity
    import MongoUtils.recursiveMerge

    def subfield:String = "emlp"

    def blessing:DBObject = MongoDBObject(subfield -> blessingData)
    def blessingData:DBObject = MongoDBObject("lb" -> null, "lu" -> new Date(0))
    def bless(id:AnyRef) {
        collection.update(
            MongoDBObject("_id" -> id, subfield -> MongoDBObject("$exists" -> false)),
            MongoDBObject("$set" -> blessing))
    }
    def blessed(o:DBObject) = recursiveMerge(o,blessing)

    def defaultLockingQueryCriteria:DBObject = MongoDBObject.empty
    def defaultLockingSortCriteria:DBObject = MongoDBObject.empty

    def lockUpdate(timeout:FiniteDuration)(implicit by:LockerIdentity):DBObject =
        MongoDBObject("$set" -> MongoDBObject(
            s"$subfield.lb" -> by.id,
            s"$subfield.lu" -> new Date(timeout.fromNow.time.toMillis)
        ))

    def lockOne(selectorCriteria:DBObject=null,sortCriteria:DBObject=null,
                updater:DBObject=null,timeout:FiniteDuration=defaultTimeout)
                (implicit by:LockerIdentity):Option[DBObject] =
        collection.findAndModify(
            recursiveMerge(defaultLockingQueryCriteria,
                MongoDBObject( s"$subfield.lu" -> MongoDBObject("$lt" -> new Date()) ),
                Option(selectorCriteria).getOrElse(MongoDBObject.empty)),
            sort=if(sortCriteria != null) sortCriteria else defaultLockingSortCriteria,
            update = if(updater!=null) recursiveMerge(lockUpdate(timeout), updater) else lockUpdate(timeout)
        )

    def insertLocked(doc:DBObject, timeout:FiniteDuration=defaultTimeout)(implicit by:LockerIdentity) {
        collection.insert(recursiveMerge(doc, MongoDBObject(subfield ->
            MongoDBObject("lb" -> by.id, "lu" -> new Date(timeout.fromNow.time.toMillis))
        )))
    }

    def ownedLockQueryCriteria(lock:DBObject)(implicit by:LockerIdentity) = MongoDBObject(
        s"$subfield.lb" -> by.id, "_id" -> lock.get("_id"),
        s"$subfield.lu" -> MongoDBObject("$gt" -> new Date())
    )

    def release(lock:DBObject, update:DBObject=null, delete:Boolean=false)(implicit by:LockerIdentity) {
        val u = MongoDBObject("$set" -> MongoDBObject(s"$subfield.lb" -> null, s"$subfield.lu" -> new Date(0)))
        (if(delete)
            collection.findAndRemove(ownedLockQueryCriteria(lock))
        else if(update != null)
            collection.findAndModify(ownedLockQueryCriteria(lock), update=recursiveMerge(u, update))
        else
            collection.findAndModify(ownedLockQueryCriteria(lock), u)
        ).orElse(throw new IllegalStateException("failure to release (not owned or late or inexistent): " + lock))
    }

    def relock(lock:DBObject,timeout:FiniteDuration=defaultTimeout)(implicit by:LockerIdentity):DBObject =
        collection.findAndModify(
            query= ownedLockQueryCriteria(lock),
            fields= MongoDBObject.empty,
            sort= MongoDBObject.empty,
            remove= false,
            update= lockUpdate(timeout),
            returnNew= true,
            upsert= false
        ).getOrElse(throw new IllegalStateException("failure to relock (not owned or late or inexistent): " + lock))
}
