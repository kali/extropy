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
    def monitor(op:Change):Set[ResolvedLocation] = {
        container.monitor(field, op)
    }
}

case class Rule(effectContainer:Container, reactionContainer:Container, tie:Tie, reaction:Reaction) {
    val reactionFields:Set[MonitoredField] = reaction.reactionFields.map( MonitoredField(reactionContainer, _) )
    val tieEffectContainerMonitoredFields:Set[MonitoredField] = tie.effectContainerMonitoredFields.map( MonitoredField(effectContainer, _) )
    val tieReactionContainerMonitoredFields:Set[MonitoredField] = tie.reactionContainerMonitoredFields.map( MonitoredField(reactionContainer, _) )

    val monitoredFields:Set[MonitoredField] = reactionFields ++ tieEffectContainerMonitoredFields ++ tieReactionContainerMonitoredFields

    def alterWrite(op:Change):Change = op // tie.alterWrite(this, op)
    def fixAll(payloadMongo:MongoClient) {
        effectContainer.asLocation.iterator(payloadMongo).foreach( fixOne(payloadMongo, _) )
    }

    def fixOne(payloadMongo:MongoClient, location:ResolvedLocation) {
        val propagated = tie.propagate(this, location)
        val resolved = propagated.resolve(payloadMongo)
        val reactant = resolved.flatMap( _.iterator(payloadMongo) ).map( _.data )
        val effects = reaction.process(reactant)
        location.setValues(payloadMongo, effects)
    }

    // returns (fieldName,expected,got)*
    def checkOne(payloadMongo:MongoClient, location:ResolvedLocation):Traversable[(String,Option[AnyRef],Option[AnyRef])] = {
        val reactant = tie.propagate(this, location).resolve(payloadMongo).flatMap( _.iterator(payloadMongo) ).map( _.data )
        val effects = reaction.process(reactant)
        location.iterator(payloadMongo).flatMap { targetDbo =>
            val target = new MongoDBObject(targetDbo.data)
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
        effectContainer.asLocation.iterator(payloadMongo).flatMap( loc => checkOne(payloadMongo, loc).map( e => (loc,e._1,e._2,e._3) ) )
    }

    def dirtiedSet(op:Change):Set[Location] =
        reactionFields.flatMap( _.monitor(op) ).flatMap( tie.backPropagate(this, _) ) ++
        tieReactionContainerMonitoredFields.flatMap( _.monitor(op) ).flatMap( tie.backPropagate(this, _) ) ++
        tieEffectContainerMonitoredFields.flatMap( _.monitor(op) )
}

// CONTAINERS

@Salat
abstract class Container {
    def asLocation:ResolvedLocation
    def monitor(field:String, op:Change):Set[ResolvedLocation]
    def toLabel:String
}

case class TopLevelContainer(collectionFullName:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def collection = collectionFullName

    def asLocation = SelectorLocation(this, MongoDBObject.empty)

    def monitor(field:String, op:Change) = {
        if(collectionFullName == op.writtenCollection)
            op match {
                case InsertChange(writtenCollection, document) =>
                    if(document.containsField(field))
                        Set(DocumentLocation(this, document))
                    else
                        Set()
                case DeleteChange(writtenCollection, selector) =>
                    Set(SelectorLocation.make(this, selector))
                case muc @ ModifiersUpdateChange(writtenCollection, selector, update) =>
                    if(muc.impactedFields.contains(field))
                        Set(SelectorLocation.make(this, selector))
                    else
                        Set()
                case FullBodyUpdateChange(writtenCollection, selector, update) =>
                    Set(SelectorLocation.make(this, selector))
            }
        else
            Set()
    }

    def toLabel = s"<i>$collectionFullName</i>"
    override def toString = collectionFullName
}

case class NestedContainer(parent:TopLevelContainer, arrayField:String) extends Container {
    def asLocation = SelectorNestedLocation(this, MongoDBObject.empty, AnySubDocumentLocationFilter)
    def monitor(field:String, op:Change) =
        parent.monitor(arrayField, op).map {
            case DocumentLocation(container, doc) => NestedDocumentLocation(this, doc, AnySubDocumentLocationFilter)
            case IdLocation(container, id) => NestedIdLocation(this, id, AnySubDocumentLocationFilter)
            case a => throw new Exception("not implemented")
        } ++ (op match {
            case muc @ ModifiersUpdateChange(writtenCollection, selector, update) =>
                if(muc.impactedFields.contains(arrayField + ".$." + field)) {
                    if(selector.contains(arrayField + "._id") && SelectorLocation.isAValue(selector.get(arrayField + "._id")))
                        Set(SelectorNestedLocation(this, selector, IdSubDocumentLocationFilter(selector.get(arrayField+"._id"))))
                    else
                        Set(SelectorNestedLocation(this, selector, AnySubDocumentLocationFilter))
                }
                else
                    Set()
            case _ => Set()
        })
    def setValues(payloadMongo:MongoClient, location:Location, values:BSONObject) {}
    def toLabel = s"<i>$parent.$arrayField</i>"
}

// LOCATIONS
sealed abstract class Location {
    def container:Container
}
sealed trait ResolvableLocation extends Location {
    def resolve(payloadMongo:MongoClient):Traversable[ResolvedLocation]
}
sealed trait ResolvedLocation extends Location with ResolvableLocation {
    def iterator(payloadMongo:MongoClient):Traversable[DataLocation]
    def setValues(payloadMongo:MongoClient, values:BSONObject)
    def resolve(payloadMongo:MongoClient) = Traversable(this)
}
sealed trait DatasLocation extends ResolvedLocation {
    def datas:Traversable[BSONObject]
}
sealed trait DataLocation extends DatasLocation {
    def data:BSONObject = datas.head
}
sealed abstract class TopLevelLocation extends Location {
    override def container:TopLevelContainer
}
sealed trait TopLevelResolvedLocation extends TopLevelLocation with ResolvedLocation {
    def selector:BSONObject
    def iterator(payloadMongo:MongoClient) = {
        val cursor = payloadMongo(container.dbName)(container.collectionName).find(selector).sort(MongoDBObject("_id" -> 1))
        cursor.option |= com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT
        cursor.toTraversable.map( DocumentLocation(container, _) )
    }
    def setValues(payloadMongo:MongoClient, values:BSONObject) {
        if(!values.isEmpty) {
            payloadMongo(container.dbName)(container.collectionName).update(
                selector, MongoDBObject("$set" -> values),
                multi=true
            )
        }
    }
}
trait HaveIdLocation extends ResolvableLocation {
    def id:AnyRef
}
case class DocumentLocation(container:TopLevelContainer, override val data:BSONObject) extends TopLevelResolvedLocation
        with DataLocation with HaveIdLocation {
    def datas = Some(data)
    def id = data.getAs[AnyRef]("_id").get
    def selector = MongoDBObject("_id" -> id)
}
case class SelectorLocation(container:TopLevelContainer, selector:BSONObject) extends TopLevelResolvedLocation
object SelectorLocation {
    def isAValue(f:AnyRef) = !f.isInstanceOf[BSONObject] && !f.isInstanceOf[java.util.regex.Pattern]
    def make(container:TopLevelContainer, selector:BSONObject) =
        if(selector.size == 1 && selector.keys.head == "_id" && isAValue(selector.values.head))
            IdLocation(container, selector.values.head)
        else if(selector.size == 1 && isAValue(selector.values.head))
            SimpleFilterLocation(container, selector.keys.head, selector.values.head)
        else
            new SelectorLocation(container, selector)
}
case class SimpleFilterLocation(container:TopLevelContainer, field:String, value:AnyRef)
        extends TopLevelResolvedLocation {
    def selector = MongoDBObject(field -> value)
}
case class IdLocation(container:TopLevelContainer, id:AnyRef)
        extends TopLevelResolvedLocation with HaveIdLocation {
    def selector = MongoDBObject("_id" -> id)
}

case class QueryLocation(container:TopLevelContainer, location:ResolvedLocation, field:String)
        extends ResolvableLocation {
    def resolve(mongo:MongoClient):Traversable[ResolvedLocation] = {
        location.iterator(mongo).flatMap( doc => doc.data.getAs[AnyRef](field).map(IdLocation(container,_)) )
    }
}
case class ShakyLocation(query:ResolvableLocation) extends Location {
    def container = query.container
    def save(mongo:MongoClient):Traversable[ResolvableLocation] = query.resolve(mongo) ++ Traversable(query)
}

sealed abstract class NestedLocation extends Location {}
object Blah { type SubDocumentLocationFilter = (BSONObject=>Boolean) }
import Blah.SubDocumentLocationFilter
case class IdSubDocumentLocationFilter(id:AnyRef) extends SubDocumentLocationFilter {
    def apply(dbo:BSONObject) = dbo.getAs[AnyRef]("_id") == Some(id)
}
case object AnySubDocumentLocationFilter extends SubDocumentLocationFilter {
    def apply(dbo:BSONObject) = true
}
case class NestedDocumentLocation(container:NestedContainer, dbo:BSONObject, filter:SubDocumentLocationFilter)
        extends NestedLocation with DataLocation {
    def datas = dbo.getAs[List[BSONObject]](container.arrayField).getOrElse(List()).filter( filter.apply(_) )
    def iterator(payloadMongo:MongoClient) = Traversable(this)
    def setValues(payloadMongo:MongoClient, values:BSONObject) {
        NestedHelpers.setValues(payloadMongo,container,MongoDBObject("_id" -> dbo.getAs[AnyRef]("_id")),filter,values)
    }
}
object NestedHelpers {
    def iteratorOnId(payloadMongo:MongoClient,container:NestedContainer, find:MongoDBObject,
            filter:SubDocumentLocationFilter):Traversable[(BSONObject,AnyRef)] =
        payloadMongo(container.parent.dbName)(container.parent.collectionName)
            .find(find).sort(MongoDBObject("_id" -> 1)).flatMap { dbo =>
                dbo.getAs[List[BSONObject]](container.arrayField).getOrElse(List())
                    .filter( filter )
                    .map { sub => (dbo, sub.getAs[AnyRef]("_id").get) }
            }.toTraversable
    def iterator(payloadMongo:MongoClient,container:NestedContainer, find:MongoDBObject,
            filter:SubDocumentLocationFilter):Traversable[NestedDocumentLocation] =
        iteratorOnId(payloadMongo, container, find, filter).map { case(dbo,id) =>
            NestedDocumentLocation(container, dbo, IdSubDocumentLocationFilter(id))
        }
    def setValues(payloadMongo:MongoClient,container:NestedContainer,find:MongoDBObject,filter:SubDocumentLocationFilter,
            values:BSONObject) {
        if(!values.isEmpty) {
            iteratorOnId(payloadMongo, container, find, filter).foreach { case(dbo,id) =>
                payloadMongo(container.parent.dbName)(container.parent.collectionName)
                    .update(MongoDBObject("_id" -> dbo.getAs[AnyRef]("_id").get, container.arrayField + "._id" -> id),
                        MongoDBObject("$set" -> MongoDBObject(values.toSeq.map {
                            case (k,v) => ((container.arrayField + ".$." + k) -> v)
                        }:_*)))
            }
        }
    }
}
case class NestedIdLocation(container:NestedContainer, id:AnyRef, filter:SubDocumentLocationFilter)
    extends NestedLocation with ResolvedLocation {
    def iterator(payloadMongo:MongoClient):Traversable[NestedDocumentLocation] =
        NestedHelpers.iterator(payloadMongo, container, MongoDBObject("_id" -> id), filter)
    def setValues(payloadMongo:MongoClient, values:BSONObject) {
        NestedHelpers.setValues(payloadMongo, container, MongoDBObject("_id" -> id), filter, values)
    }
}

case class SimpleNestedLocation(container:NestedContainer, field:String, value:AnyRef)
        extends NestedLocation with ResolvedLocation {
    def iterator(payloadMongo:MongoClient) =
        NestedHelpers.iterator(payloadMongo, container, MongoDBObject(container.arrayField+"."+field -> value),
            dbo=>dbo.getAs[AnyRef](field)==Some(value))
    def setValues(payloadMongo:MongoClient, values:BSONObject) =
        NestedHelpers.setValues(payloadMongo, container, MongoDBObject(container.arrayField+"."+field -> value),
            dbo=>dbo.getAs[AnyRef](field)==Some(value), values)
}

case class SelectorNestedLocation(container:NestedContainer, selector:MongoDBObject, filter:SubDocumentLocationFilter)
        extends NestedLocation with ResolvedLocation {
    val augmentedSelector:MongoDBObject = new MongoDBObject(
        Map(container.arrayField + "._id" -> MongoDBObject("$exists" -> true)) ++ selector
    )
    def iterator(payloadMongo:MongoClient) =
        NestedHelpers.iterator(payloadMongo, container, augmentedSelector, filter)
    def setValues(payloadMongo:MongoClient, values:BSONObject) =
        NestedHelpers.setValues(payloadMongo, container, augmentedSelector, filter, values)
}
// TIE

@Salat
abstract class Tie {
    def reactionContainerMonitoredFields:Set[String]
    def effectContainerMonitoredFields:Set[String]

    def propagate(rule:Rule, from:ResolvedLocation):ResolvableLocation
    def backPropagate(rule:Rule, location:ResolvedLocation):Traversable[Location]

    def toLabel:String
}

case class SameDocumentTie() extends Tie {
    def reactionContainerMonitoredFields = Set()
    def effectContainerMonitoredFields = Set()

    def propagate(rule:Rule, from:ResolvedLocation):ResolvableLocation = from
    def backPropagate(rule:Rule, location:ResolvedLocation):Traversable[Location] = Some(location)

    def toLabel = "from the same document in"
}

case class FollowKeyTie(localFieldName:String) extends Tie {
    val effectContainerMonitoredFields = Set(localFieldName)
    val reactionContainerMonitoredFields:Set[String] = Set()

    def propagate(rule:Rule, from:ResolvedLocation) = from match {
        case data:DataLocation => IdLocation(rule.reactionContainer.asInstanceOf[TopLevelContainer],
                                        data.data.getAs[AnyRef](localFieldName).get)
        case tld:ResolvedLocation => QueryLocation(rule.reactionContainer.asInstanceOf[TopLevelContainer],
                                                tld, localFieldName)
        case _ => throw new Exception(s"Unexpected location:$from in propagate for $this")
    }
    def backPropagate(rule:Rule, location:ResolvedLocation):Traversable[Location] = rule.effectContainer match {
        case cc:TopLevelContainer => location.asInstanceOf[TopLevelLocation] match {
            case hil:HaveIdLocation => Some(SimpleFilterLocation(cc, localFieldName, hil.id))
            case tld:TopLevelResolvedLocation => Some(QueryLocation(cc, tld, "_id"))
            case _ => throw new Exception(s"Unexpected location:$location in backPropagate for $this")
        }
        case cc:NestedContainer => location.asInstanceOf[TopLevelLocation] match {
            case hil:HaveIdLocation => Some(SimpleNestedLocation(cc, localFieldName, hil.id))
            case _ => throw new Exception(s"Unexpected location:$location in backPropagate for $this")
        }
    }
    def toLabel = s"following <i>$localFieldName</i> to"
}

case class ReverseKeyTie(reactionFieldName:String) extends Tie {
    val effectContainerMonitoredFields:Set[String] = Set("_id")
    val reactionContainerMonitoredFields = Set(reactionFieldName)
    def propagate(rule:Rule, location:ResolvedLocation) = rule.reactionContainer match {
        case cc:TopLevelContainer => location match {
            case hil:HaveIdLocation => SimpleFilterLocation(cc, reactionFieldName, hil.id)
            case tld:TopLevelResolvedLocation => QueryLocation(cc, tld, "_id")
            case _ => throw new Exception(s"Unexpected location:$location in backPropagate for $this")
        }
        case cc@ NestedContainer(collection, field) => location match {
            case hil:HaveIdLocation => SimpleNestedLocation(cc, reactionFieldName, hil.id)
            case _ => throw new Exception(s"Unexpected location:$location in backPropagate for $this")
        }
    }
    def backPropagate(rule:Rule, location:ResolvedLocation):Traversable[Location] = {
        val q = location match {
            case sel:ResolvedLocation =>
                QueryLocation(rule.effectContainer.asInstanceOf[TopLevelContainer], sel, reactionFieldName)
            case _ => throw new Exception(s"Unexpected location:$location in backPropagate for $this")
        }
        Traversable(ShakyLocation(q))
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
        Rule(TopLevelContainer(collection), TopLevelContainer(collection), SameDocumentTie(),
                StringNormalizationReaction(from,to))
}

class InvariantDAO(val db:MongoDB, val lockDuration:FiniteDuration)(implicit ctx: com.novus.salat.Context) {
    val collection = db("invariants")
    val salat = new SalatDAO[Invariant,ObjectId](collection) {}
    val mlp = MongoLockingPool(collection, lockDuration)
}
