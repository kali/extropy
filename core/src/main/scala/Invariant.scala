package org.zoy.kali.extropy

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._

import scala.concurrent.duration._

import mongoutils._

abstract sealed class Change {
    def writtenCollection:String
}

case class FullBodyUpdateChange(writtenCollection:String, selector:BSONObject, update:BSONObject) extends Change
case class InsertChange(writtenCollection:String, documents:Stream[BSONObject]) extends Change
case class DeleteChange(writtenCollection:String, selector:BSONObject) extends Change
case class ModifiersUpdateChange(writtenCollection:String, selector:BSONObject, update:BSONObject) extends Change {
    def impactedFields:Set[String] = update.asInstanceOf[DBObject].values.flatMap( _.asInstanceOf[DBObject].keys ).toSet
}


case class Invariant(   _id:ObjectId, rule:Rule, emlp:MongoLock, statusChanging:Boolean=false,
                        status:InvariantStatus.Value=InvariantStatus.Created,
                        command:Option[InvariantStatus.Value]=None)

object Invariant {
    def apply(rule:Rule) = new Invariant(new ObjectId(), rule, MongoLock.empty)
}

object InvariantStatus extends Enumeration {
    val Created = Value("created")
    val Stop = Value("stop")
    val Sync = Value("sync")                // all proxies are "sync", foreman syncs actively
    val Run = Value("run")                  // all proxies are "run"
    val Error = Value("error")
}

case class MonitoredField(container:Container, field:String) {

    def monitor(op:Change):Set[Location] =
        if(container.collection == op.writtenCollection)
            op match {
                case InsertChange(writtenCollection, documents) => documents.filter( _.containsField(field) )
                        .map( d => DocumentLocation(d.asInstanceOf[DBObject]) ).toSet
                case DeleteChange(writtenCollection, selector) =>
                    Set(SelectorLocation(selector.asInstanceOf[DBObject]))
                case muc @ ModifiersUpdateChange(writtenCollection, selector, update) =>
                    if(muc.impactedFields.contains(field))
                        Set(SelectorLocation(selector.asInstanceOf[DBObject]))
                    else
                        Set()
                case FullBodyUpdateChange(writtenCollection, selector, update) =>
                    Set(SelectorLocation(selector.asInstanceOf[DBObject]))
            }
        else
            Set()

}

case class Rule(effectContainer:Container, tie:Tie, reaction:Reaction) {
    val reactionFields:Set[MonitoredField] = reaction.reactionFields.map( MonitoredField(tie.reactionContainer, _) )
    val tieEffectContainerMonitoredFields:Set[MonitoredField] = tie.effectContainerMonitoredFields.map( MonitoredField(effectContainer, _) )
    val tieReactionContainerMonitoredFields:Set[MonitoredField] = tie.reactionContainerMonitoredFields.map( MonitoredField(tie.reactionContainer, _) )

    val monitoredFields:Set[MonitoredField] = reactionFields ++ tieEffectContainerMonitoredFields ++ tieReactionContainerMonitoredFields

    def alterWrite(op:Change):Change = op // tie.alterWrite(this, op)
    def activeSync(extropy:BaseExtropyContext) {
    }

    def dirtiedSet(op:Change):Set[Location] =
        reactionFields.flatMap( _.monitor(op) ).map( tie.backPropagate(_) ) ++
        tieReactionContainerMonitoredFields.flatMap( _.monitor(op) ).map( tie.backPropagate(_) ) ++
        tieEffectContainerMonitoredFields.flatMap( _.monitor(op) )

}

// CONTAINERS

@Salat
abstract class Container {
    def iterator(payloadMongo:MongoClient):Traversable[Location]
    def collection:String
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject)
    def dbName:String
    def collectionName:String
}

case class CollectionContainer(collectionFullName:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def collection = collectionFullName
    def iterator(payloadMongo:MongoClient) = {
        val cursor = payloadMongo(dbName)(collectionName).find(MongoDBObject.empty).sort(MongoDBObject("_id" -> 1))
        cursor.option |= com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT
        cursor.toTraversable.map( DocumentLocation(_) )
    }
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject) {
/*
        payloadMongo(dbName)(collectionName).update(
            MongoDBObject("_id" -> location.dbo.get("_id")),
            MongoDBObject("$set" -> values)
        )
*/
    }
}

case class SubCollectionContainer(collectionFullName:String, arrayField:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def iterator(payloadMongo:MongoClient):Traversable[Location] = null
    def collection:String = collectionFullName
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject) {}
}

// LOCATION

abstract class Location {
    def asById:Option[AnyRef]
    def asSelector:Option[DBObject]
    def save(extropy:BaseExtropyContext):Location
}
case class DocumentLocation(dbo:DBObject) extends Location {
    override def asById:Option[AnyRef] = dbo.getAs[AnyRef]("_id")
    override def asSelector:Option[DBObject] = asById.map( id => MongoDBObject("_id" -> id) )
    def save(extropy:BaseExtropyContext):Location = this
}
case class SelectorLocation(selector:DBObject) extends Location {
    override def asById:Option[AnyRef] =
        if(selector.size == 1 && selector.keys.head == "_id" &&
            !selector.values.head.isInstanceOf[DBObject] &&
            !selector.values.head.isInstanceOf[java.util.regex.Pattern])
            Some(selector.values.head)
        else
            None
    override def asSelector:Option[DBObject] = Some(selector)
    def save(extropy:BaseExtropyContext):Location = this
}
case class BeforeAndAfterIdLocation(container:Container, selector:DBObject, field:String) extends Location {
    def asById:Option[AnyRef] = None
    def asSelector = throw new NotImplementedError
    def save(extropy:BaseExtropyContext):Location =
        SelectorLocation(MongoDBObject("_id" -> MongoDBObject("$in" -> extropy.payloadMongo(container.dbName)(container.collectionName).find(selector,MongoDBObject(field -> 1)).toSeq)))
}

// TIE

@Salat
abstract class Tie {
    def reactionContainer:Container
    def effectContainerMonitoredFields:Set[String]
    def reactionContainerMonitoredFields:Set[String]

    def resolve(from:Location):Location
    def backPropagate(location:Location):Location
}

case class SameDocumentTie(container:Container) extends Tie {
    def reactionContainer:Container = container
    def effectContainerMonitoredFields = Set()
    def reactionContainerMonitoredFields = Set()

    def resolve(from:Location) = from
    def backPropagate(location:Location):Location = location
}

case class FollowKeyTie(collectionName:String, localFieldName:String) extends Tie {
    def reactionContainer:Container = CollectionContainer(collectionName)
    val effectContainerMonitoredFields = Set(localFieldName)
    val reactionContainerMonitoredFields:Set[String] = Set()
    def resolve(from:Location) = from match {
        case DocumentLocation(doc) => SelectorLocation(MongoDBObject("_id" -> doc.getAs[AnyRef](localFieldName)))
    }
    def backPropagate(location:Location):Location = location.asById match {
        case Some(id) => SelectorLocation(MongoDBObject(localFieldName -> id))
    }
}

case class ReverseKeyTie(container:Container, reactionFieldName:String) extends Tie {
    def reactionContainer:Container = container
    val effectContainerMonitoredFields:Set[String] = Set("_id")
    val reactionContainerMonitoredFields = Set(reactionFieldName)
    def resolve(from:Location) = from match {
        case DocumentLocation(doc) => SelectorLocation(MongoDBObject(reactionFieldName -> doc.getAs[AnyRef]("_id")))
    }
    def backPropagate(location:Location):Location = BeforeAndAfterIdLocation(container, location.asSelector.get, reactionFieldName)
}

// PROCESSORS
@Salat abstract class Reaction {
    def reactionFields:Set[String]
    def process(data:Traversable[DBObject]):DBObject
}

case class CopyField(from:String, to:String)
case class CopyFieldsReaction(fields:List[CopyField]) extends Reaction {
    val reactionFields:Set[String] = fields.map( _.from ).toSet
    def process(data:Traversable[DBObject]) = MongoDBObject.empty
}

case class CountReaction(field:String) extends Reaction {
    val reactionFields:Set[String] = Set()
    def process(data:Traversable[DBObject]) = MongoDBObject(field -> data.size)
}

// FOR TESTS

case class StringNormalizationReaction(from:String, to:String) extends Reaction {
    val reactionFields:Set[String] = Set(from)
    def process(data:Traversable[DBObject]) = Map(to -> (data.headOption match {
        case Some(obj) => obj.get(from).toString.toLowerCase
        case None => null
    }))
}


object StringNormalizationRule {
    def apply(collection:String, from:String, to:String) =
        Rule(CollectionContainer(collection), SameDocumentTie(CollectionContainer(collection)),
                StringNormalizationReaction(from,to))
}

class InvariantDAO(val db:MongoDB, val lockDuration:FiniteDuration)(implicit ctx: com.novus.salat.Context) {
    val collection = db("invariants")
    val salat = new SalatDAO[Invariant,ObjectId](collection) {}
    val mlp = MongoLockingPool(collection, lockDuration)
}
