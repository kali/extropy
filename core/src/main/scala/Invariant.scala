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
                    Set(SelectorLocation(selector.asInstanceOf[DBObject]).optimize)
                case muc @ ModifiersUpdateChange(writtenCollection, selector, update) =>
                    if(muc.impactedFields.contains(field))
                        Set(SelectorLocation(selector.asInstanceOf[DBObject]).optimize)
                    else
                        Set()
                case FullBodyUpdateChange(writtenCollection, selector, update) =>
                    Set(SelectorLocation(selector.asInstanceOf[DBObject]).optimize)
            }
        else
            Set()

}

case class Rule(effectContainer:Container, reactionContainer:Container, tie:Tie, reaction:Reaction) {
    val reactionFields:Set[MonitoredField] = reaction.reactionFields.map( MonitoredField(reactionContainer, _) )
    val tieEffectContainerMonitoredFields:Set[MonitoredField] = tie.effectContainerMonitoredFields.map( MonitoredField(effectContainer, _) )
    val tieReactionContainerMonitoredFields:Set[MonitoredField] = tie.reactionContainerMonitoredFields.map( MonitoredField(reactionContainer, _) )

    val monitoredFields:Set[MonitoredField] = reactionFields ++ tieEffectContainerMonitoredFields ++ tieReactionContainerMonitoredFields

    def alterWrite(op:Change):Change = op // tie.alterWrite(this, op)
    def activeSync(extropy:BaseExtropyContext) {
    }

    def fixOne(payloadMongo:MongoClient, location:Location) {
        val reactant = reactionContainer.pull(payloadMongo, tie.resolve(this, location))
        val effects = reaction.process(reactant)
        effectContainer.setValues(payloadMongo, location, effects)
    }

    def dirtiedSet(op:Change):Set[Location] =
        reactionFields.flatMap( _.monitor(op) ).map( tie.backPropagate(this, _) ) ++
        tieReactionContainerMonitoredFields.flatMap( _.monitor(op) ).map( tie.backPropagate(this, _) ) ++
        tieEffectContainerMonitoredFields.flatMap( _.monitor(op) )

}

// CONTAINERS

@Salat
abstract class Container {
    def collection:String
    def dbName:String
    def collectionName:String

    def iterator(payloadMongo:MongoClient):Traversable[Location]
    def pull(payloadMongo:MongoClient, loc:Location):Iterable[DBObject]

    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject)
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
    def pull(payloadMongo:MongoClient, loc:Location):Iterable[DBObject] = loc.expand(payloadMongo).flatMap { expanded =>
        payloadMongo(dbName)(collectionName).find(expanded.asSelectorLocation.selector)
    }

    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject) {
        payloadMongo(dbName)(collectionName).update(
            MongoDBObject("_id" -> location.asIdLocation.id),
            MongoDBObject("$set" -> values)
        )
    }
}

case class SubCollectionContainer(collectionFullName:String, arrayField:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def iterator(payloadMongo:MongoClient):Traversable[Location] = null
    def pull(payloadMongo:MongoClient, loc:Location):Iterable[DBObject] = null
    def collection:String = collectionFullName
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject) {}
}

// LOCATIONS

abstract class Location {
    def asIdLocation:IdLocation
    def asSelectorLocation:SelectorLocation
    def expand(extropy:MongoClient):Iterable[Location]
    def optimize:Location = this
}
case class DocumentLocation(dbo:DBObject) extends Location {
    override def asIdLocation:IdLocation = IdLocation(dbo.getAs[AnyRef]("_id").get)
    override def asSelectorLocation:SelectorLocation = asIdLocation.asSelectorLocation
    def expand(extropy:MongoClient):Iterable[Location] = Some(this)
}
case class SelectorLocation(selector:DBObject) extends Location {
    def asIdLocationOption = if(selector.size == 1 && selector.keys.head == "_id" &&
            !selector.values.head.isInstanceOf[DBObject] &&
            !selector.values.head.isInstanceOf[java.util.regex.Pattern])
            Some(IdLocation(selector.values.head))
        else
            None
    override def asIdLocation:IdLocation = asIdLocationOption.get
    override def asSelectorLocation = this
    override def optimize = asIdLocationOption.getOrElse(this)
    def expand(extropy:MongoClient):Iterable[Location] = Some(this)
}
case class IdLocation(id:AnyRef) extends Location {
    def asSelectorLocation = SelectorLocation(MongoDBObject("_id" -> id))
    def asIdLocation = this
    def expand(mongo:MongoClient):Iterable[Location] = Some(this)
}
case class QueryLocation(container:Container, location:Location, field:String) extends Location {
    def asIdLocation = throw new IllegalStateException("can't make IdLocation from: " + this.toString)
    def asSelectorLocation = throw new IllegalStateException("can't make SelectorLocation from: " + this.toString)
    def expand(mongo:MongoClient):Iterable[Location] = container.pull(mongo, location).headOption.map( doc => IdLocation(doc.getAs[AnyRef](field)) )
}
case class BeforeAndAfterIdLocation(container:Container, location:Location, field:String) extends Location {
    def asIdLocation = throw new IllegalStateException
    def asSelectorLocation = throw new IllegalStateException
    def expand(mongo:MongoClient):Iterable[Location] =
        Some(SelectorLocation(MongoDBObject("_id" -> MongoDBObject("$in" -> mongo(container.dbName)(container.collectionName).find(location.asSelectorLocation.selector,MongoDBObject(field -> 1)).toSeq))).optimize)
}

// TIE

@Salat
abstract class Tie {
    def reactionContainerMonitoredFields:Set[String]
    def effectContainerMonitoredFields:Set[String]

    def resolve(rule:Rule, from:Location):Location
    def backPropagate(rule:Rule, location:Location):Location
}

case class SameDocumentTie extends Tie {
    def reactionContainerMonitoredFields = Set()
    def effectContainerMonitoredFields = Set()

    def resolve(rule:Rule, from:Location) = from
    def backPropagate(rule:Rule, location:Location):Location = location.optimize
}

case class FollowKeyTie(localFieldName:String) extends Tie {
    val effectContainerMonitoredFields = Set(localFieldName)
    val reactionContainerMonitoredFields:Set[String] = Set()

    def resolve(rule:Rule, from:Location) = from match {
        case DocumentLocation(doc) => IdLocation(doc.getAs[AnyRef](localFieldName).get)
        case idl:IdLocation => QueryLocation(rule.effectContainer, idl, localFieldName)
    }
    def backPropagate(rule:Rule, location:Location):Location = SelectorLocation(MongoDBObject(localFieldName -> location.asIdLocation.id)).optimize
}

case class ReverseKeyTie(reactionFieldName:String) extends Tie {
    val effectContainerMonitoredFields:Set[String] = Set("_id")
    val reactionContainerMonitoredFields = Set(reactionFieldName)
    def resolve(rule:Rule, from:Location) = SelectorLocation(MongoDBObject(reactionFieldName -> from.asIdLocation.id)).optimize
    def backPropagate(rule:Rule, location:Location):Location = BeforeAndAfterIdLocation(rule.reactionContainer, location.asSelectorLocation.optimize, reactionFieldName)
}

// REACTION
@Salat abstract class Reaction {
    def reactionFields:Set[String]
    def process(data:Traversable[DBObject]):DBObject
}

case class CopyField(from:String, to:String)
case class CopyFieldsReaction(fields:List[CopyField]) extends Reaction {
    val reactionFields:Set[String] = fields.map( _.from ).toSet
    def process(data:Traversable[DBObject]) = data.headOption.map { doc =>
        MongoDBObject(fields.map { pair => (pair.to, doc.getAs[AnyRef](pair.from)) })
    }.getOrElse(MongoDBObject.empty)
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
        Rule(CollectionContainer(collection), CollectionContainer(collection), SameDocumentTie(),
                StringNormalizationReaction(from,to))
}

class InvariantDAO(val db:MongoDB, val lockDuration:FiniteDuration)(implicit ctx: com.novus.salat.Context) {
    val collection = db("invariants")
    val salat = new SalatDAO[Invariant,ObjectId](collection) {}
    val mlp = MongoLockingPool(collection, lockDuration)
}
