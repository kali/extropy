package org.zoy.kali.extropy

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._

import scala.concurrent.duration._

import mongoutils._

import mongoutils.BSONObjectConversions._

abstract sealed class Change {
    def writtenCollection:String
    def play(payloadMongo:MongoClient):WriteResult

    def dbName = writtenCollection.split('.').head
    def collectionName = writtenCollection.split('.').drop(1).mkString(".")
}

case class FullBodyUpdateChange(writtenCollection:String, selector:BSONObject, update:BSONObject) extends Change {
    def play(payloadMongo:MongoClient):WriteResult = payloadMongo(dbName)(collectionName).update(selector,update)
}
case class InsertChange(writtenCollection:String, document:BSONObject) extends Change {
    def play(payloadMongo:MongoClient):WriteResult = payloadMongo(dbName)(collectionName).insert(document)
}
case class DeleteChange(writtenCollection:String, selector:BSONObject) extends Change {
    def play(payloadMongo:MongoClient):WriteResult = payloadMongo(dbName)(collectionName).remove(selector)
}
case class ModifiersUpdateChange(writtenCollection:String, selector:BSONObject, update:BSONObject) extends Change {
    def impactedFields:Set[String] = update.values.flatMap( _.asInstanceOf[BSONObject].keys ).toSet
    def play(payloadMongo:MongoClient):WriteResult = payloadMongo(dbName)(collectionName).update(selector, update)
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
                case InsertChange(writtenCollection, document) => container match {
                    case CollectionContainer(collection) =>
                        if(document.containsField(field))
                            Set(DocumentLocation(CollectionContainer(collection), document))
                        else
                            Set()
/*
                    case SubCollectionContainer(collection, arrayField) =>
                        if(document.containsField(arrayField) &&
                            document.getAs[List[BSONObject]](arrayField).get.exists(subdoc => subdoc.containsField(field)) )
                            Set(SubDocumentLocation(DocumentLocation(document), SelectorLocation(MongoDBObject.empty)))
                        else
                            Set()
*/
                }
                case DeleteChange(writtenCollection, selector) =>
                    Set(SelectorLocation(CollectionContainer(writtenCollection), selector).optimize)
                case muc @ ModifiersUpdateChange(writtenCollection, selector, update) =>
                    if(muc.impactedFields.contains(field))
                        Set(SelectorLocation(CollectionContainer(writtenCollection), selector).optimize)
                    else
                        Set()
                case FullBodyUpdateChange(writtenCollection, selector, update) =>
                    Set(SelectorLocation(CollectionContainer(writtenCollection), selector).optimize)
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
    def fixAll(payloadMongo:MongoClient) {
        effectContainer.iterator(payloadMongo).foreach( fixOne(payloadMongo, _) )
    }

    def fixOne(payloadMongo:MongoClient, location:Location) {
        val reactant = reactionContainer.pull(payloadMongo, tie.resolve(this, location))
        val effects = reaction.process(reactant)
        effectContainer.setValues(payloadMongo, location, effects)
    }

    // returns (fieldName,expected,got)*
    def checkOne(payloadMongo:MongoClient, location:Location):Iterable[(String,Option[AnyRef],Option[AnyRef])] = {
        val reactant = reactionContainer.pull(payloadMongo, tie.resolve(this, location))
        val effects = reaction.process(reactant)
        effectContainer.pull(payloadMongo, location).flatMap { targetDbo =>
            val target = new MongoDBObject(targetDbo)
            new MongoDBObject(effects).flatMap{ case (k,v) =>
                val got = target.getAs[AnyRef](k)
                if(got != Option(v))
                    Some(k,Option(v),got)
                else
                    None
            }
        }
    }

    def checkAll(payloadMongo:MongoClient):Traversable[(Location, String, AnyRef,AnyRef)] = {
        effectContainer.iterator(payloadMongo).flatMap( loc => checkOne(payloadMongo, loc).map( e => (loc,e._1,e._2,e._3) ) )
    }

    def dirtiedSet(op:Change):Set[Location] =
        reactionFields.flatMap( _.monitor(op) ).flatMap( tie.backPropagate(this, _) ) ++
        tieReactionContainerMonitoredFields.flatMap( _.monitor(op) ).flatMap( tie.backPropagate(this, _) ) ++
        tieEffectContainerMonitoredFields.flatMap( _.monitor(op) )

}

// CONTAINERS

@Salat
abstract class Container {
    def collection:String
    def dbName:String
    def collectionName:String

    def iterator(payloadMongo:MongoClient):Traversable[Location]
    def pull(payloadMongo:MongoClient, loc:Location):Iterable[BSONObject]

    def setValues(payloadMongo:MongoClient, location:Location, values:BSONObject)

    def toLabel:String
}

case class CollectionContainer(collectionFullName:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def collection = collectionFullName
    def iterator(payloadMongo:MongoClient) = {
        val cursor = payloadMongo(dbName)(collectionName).find(MongoDBObject.empty).sort(MongoDBObject("_id" -> 1))
        cursor.option |= com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT
        cursor.toTraversable.map( DocumentLocation(this, _) )
    }
    def pull(payloadMongo:MongoClient, loc:Location):Iterable[BSONObject] = loc.expand(payloadMongo).flatMap { expanded =>
        payloadMongo(dbName)(collectionName).find(expanded.asSelectorLocation.selector)
    }

    def setValues(payloadMongo:MongoClient, location:Location, values:BSONObject) {
        if(!values.isEmpty) {
            payloadMongo(dbName)(collectionName).update(
                location.asSelectorLocation.selector,
                MongoDBObject("$set" -> values),
                multi=true
            )
        }
    }

    def toLabel = s"<i>$collectionFullName</i>"
}

case class SubCollectionContainer(collectionFullName:String, arrayField:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def iterator(payloadMongo:MongoClient):Traversable[Location] = {
        val cursor = payloadMongo(dbName)(collectionName).find(MongoDBObject.empty).sort(MongoDBObject("_id" -> 1))
        cursor.option |= com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT
/*
        cursor.toTraversable.flatMap { doc =>
            doc.getAs[List[BSONObject]](arrayField).map { list =>
                list.map( subdoc => SubDocumentLocation(DocumentLocation(doc), IdLocation(subdoc.getAs[AnyRef]("_id").get)) )
            }.getOrElse(List())
        }
*/
        List()
    }
    def pull(payloadMongo:MongoClient, loc:Location):Iterable[BSONObject] = loc match {
        case subloc:SubDocumentLocation =>
            val main:Iterable[BSONObject] = payloadMongo(dbName)(collectionName).find(subloc.topLevelLoc.asSelectorLocation.selector).toIterable
            val subList:Iterable[BSONObject] = main.flatMap( _.getAs[Iterable[BSONObject]](arrayField).getOrElse(Iterable()) )
            subList.filter( loc.asInstanceOf[SubDocumentLocation].nested.matches( _ ) )
        case _ => CollectionContainer(collectionFullName).pull(payloadMongo, loc).flatMap { doc =>
             doc.getAs[Iterable[BSONObject]](arrayField).getOrElse(Iterable())
        }
    }
    def collection:String = collectionFullName
    def setValues(payloadMongo:MongoClient, location:Location, values:BSONObject) {}
    def toLabel = s"<i>$collectionFullName.$arrayField</i>"
}

// LOCATIONS

abstract class Location {
    def asIdLocation:IdLocation
    def asSelectorLocation:SelectorLocation
    def expand(extropy:MongoClient):Iterable[Location]
    def optimize:Location = this
    def snapshot(mongo:MongoClient):Iterable[Location] = Some(this)
    def matches(obj:BSONObject):Boolean
}
abstract class TopLevelLocation extends Location {
    def container:CollectionContainer
}

case class DocumentLocation(container:CollectionContainer, dbo:BSONObject) extends TopLevelLocation {
    override def asIdLocation:IdLocation = IdLocation(container, dbo.getAs[AnyRef]("_id").get)
    override def asSelectorLocation:SelectorLocation = asIdLocation.asSelectorLocation
    def expand(extropy:MongoClient):Iterable[Location] = Some(this)
    def matches(obj:BSONObject) = obj.getAs[AnyRef]("_id").get == dbo.getAs[AnyRef]("_id").get
}
case class SelectorLocation(container:CollectionContainer, selector:BSONObject) extends TopLevelLocation {
    def asIdLocationOption = if(selector.size == 1 && selector.keys.head == "_id" &&
            !selector.values.head.isInstanceOf[BSONObject] &&
            !selector.values.head.isInstanceOf[java.util.regex.Pattern])
            Some(IdLocation(container, selector.values.head))
        else
            None
    def asSimpleFilterLocationOption = if(selector.size == 1 &&
            !selector.values.head.isInstanceOf[BSONObject] &&
            !selector.values.head.isInstanceOf[java.util.regex.Pattern])
            Some(SimpleFilterLocation(container, selector.keys.head, selector.values.head))
        else
            None
    override def asIdLocation:IdLocation = asIdLocationOption.get
    override def asSelectorLocation = this
    override def optimize = asIdLocationOption orElse asSimpleFilterLocationOption getOrElse this
    def expand(extropy:MongoClient):Iterable[Location] = Some(this)
    def matches(obj:BSONObject) = throw new IllegalStateException()
}
case class SimpleFilterLocation(container:CollectionContainer, field:String, value:AnyRef) extends TopLevelLocation {
    def asSelectorLocation = SelectorLocation(container, MongoDBObject(field -> value))
    def asIdLocation = if(field == "_id") IdLocation(container, value) else throw new IllegalStateException
    def expand(mongo:MongoClient):Iterable[Location] = Some(this)
    def matches(obj:BSONObject) = obj.getAs[AnyRef](field) == Some(value)
}
case class IdLocation(container:CollectionContainer, id:AnyRef) extends TopLevelLocation {
    def asSelectorLocation = SelectorLocation(container, MongoDBObject("_id" -> id))
    def asIdLocation = this
    def expand(mongo:MongoClient):Iterable[Location] = Some(this)
    def matches(obj:BSONObject) = obj.getAs[AnyRef]("_id").get == id
}

case class QueryLocation(container:Container, location:Location, field:String) extends Location {
    def asIdLocation = throw new IllegalStateException("can't make IdLocation from: " + this.toString)
    def asSelectorLocation = throw new IllegalStateException("can't make SelectorLocation from: " + this.toString)
    def expand(mongo:MongoClient):Iterable[Location] = {
        container.pull(mongo, location).flatMap( doc => doc.getAs[AnyRef](field).map(IdLocation(null,_)) )
    }
    def matches(obj:BSONObject) = throw new IllegalStateException()
}
case class SnapshotLocation(query:QueryLocation) extends Location {
    def asIdLocation = throw new IllegalStateException
    def asSelectorLocation = throw new IllegalStateException
    override def snapshot(mongo:MongoClient) = query.expand(mongo)
    def expand(mongo:MongoClient):Iterable[Location] = throw new IllegalStateException
    def matches(obj:BSONObject) = throw new IllegalStateException()
}

case class SubDocumentLocation(topLevelLoc:Location, nested:Location) extends Location {
    override def asIdLocation:IdLocation = throw new IllegalStateException("can't make IdLocation from:" + this.toString)
    override def asSelectorLocation:SelectorLocation = throw new IllegalStateException("can't make SelectorLocation from:" + this.toString)
    def expand(extropy:MongoClient):Iterable[Location] = Some(this)
    def matches(obj:BSONObject) = throw new IllegalStateException()
}
// TIE

@Salat
abstract class Tie {
    def reactionContainerMonitoredFields:Set[String]
    def effectContainerMonitoredFields:Set[String]

    def resolve(rule:Rule, from:Location):Location
    def backPropagate(rule:Rule, location:Location):Iterable[Location]

    def toLabel:String
}

case class SameDocumentTie() extends Tie {
    def reactionContainerMonitoredFields = Set()
    def effectContainerMonitoredFields = Set()

    def resolve(rule:Rule, from:Location) = from
    def backPropagate(rule:Rule, location:Location):Iterable[Location] = Some(location.optimize)

    def toLabel = "from the same document in"
}

case class FollowKeyTie(localFieldName:String) extends Tie {
    val effectContainerMonitoredFields = Set(localFieldName)
    val reactionContainerMonitoredFields:Set[String] = Set()

    def resolve(rule:Rule, from:Location) = from match {
        case DocumentLocation(_, doc) => IdLocation(rule.reactionContainer.asInstanceOf[CollectionContainer],
                                        doc.getAs[AnyRef](localFieldName).get)
/*
        case SelectorLocation(sel) => IdLocation(rule.effectContainer.asInstanceOf[CollectionContainer],
                                        sel.getAs[AnyRef](localFieldName).get)
*/
        case idl:IdLocation => QueryLocation(rule.effectContainer, idl, localFieldName)
//        case SubDocumentLocation(_,DocumentLocation(subdoc)) => IdLocation(subdoc.getAs[AnyRef](localFieldName).get)
    }
    def backPropagate(rule:Rule, location:Location):Iterable[Location] = rule.effectContainer match {
        case a:CollectionContainer => Some(SimpleFilterLocation(a, localFieldName, location.asIdLocation.id))
        case SubCollectionContainer(_,field) => null
/*
            Some(SubDocumentLocation(   SimpleFilterLocation(field + "." + localFieldName, location.asIdLocation.id),
                                        SimpleFilterLocation(localFieldName, location.asIdLocation.id)))
*/
    }
    def toLabel = s"following <i>$localFieldName</i> to"
}

case class ReverseKeyTie(reactionFieldName:String) extends Tie {
    val effectContainerMonitoredFields:Set[String] = Set("_id")
    val reactionContainerMonitoredFields = Set(reactionFieldName)
    def resolve(rule:Rule, from:Location) = rule.reactionContainer match {
        case cc:CollectionContainer => SimpleFilterLocation(cc, reactionFieldName, from.asIdLocation.id)
        case SubCollectionContainer(collection, field) =>
            SubDocumentLocation(
                SimpleFilterLocation(null, field + "." + reactionFieldName, from.asIdLocation.id),
                SimpleFilterLocation(null, reactionFieldName, from.asIdLocation.id))
    }
    def backPropagate(rule:Rule, location:Location):Iterable[Location] = {
        val q = QueryLocation(rule.reactionContainer, location.asSelectorLocation.optimize, reactionFieldName)
        Array(SnapshotLocation(q), q)
    }
    def toLabel = s"searching by <i>$reactionFieldName</i> in"
}

// REACTION
@Salat abstract class Reaction {
    def reactionFields:Set[String]
    def process(data:Traversable[BSONObject]):BSONObject
    def toLabel:String
}

case class CopyField(from:String, to:String)
case class CopyFieldsReaction(fields:List[CopyField]) extends Reaction {
    val reactionFields:Set[String] = fields.map( _.from ).toSet
    def process(data:Traversable[BSONObject]) = data.headOption.map { doc =>
        MongoDBObject(fields.map { pair => (pair.to, doc.getAs[AnyRef](pair.from)) })
    }.getOrElse(MongoDBObject.empty)
    def toLabel = "copy " + fields.map( f => "<i>%s</i> as <i>%s</i>".format(f.from, f.to) ).mkString(", ")
}

case class CountReaction(field:String) extends Reaction {
    val reactionFields:Set[String] = Set()
    def process(data:Traversable[BSONObject]) = MongoDBObject(field -> data.size)
    def toLabel = s"count as <i>$field</i>"
}

// FOR TESTS

case class StringNormalizationReaction(from:String, to:String) extends Reaction {
    val reactionFields:Set[String] = Set(from)
    def process(data:Traversable[BSONObject]) = Map(to -> (data.headOption match {
        case Some(obj) => obj.get(from).toString.toLowerCase
        case None => null
    }))
    def toLabel = s"normalize <i>$from</i> as <i>$to</i>"
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
