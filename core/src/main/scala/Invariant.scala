package org.zoy.kali.extropy

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._

import scala.concurrent.duration._

import mongoutils._

import mongoutils.BSONObjectConversions._

import com.typesafe.scalalogging.slf4j.StrictLogging

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

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

case class Rule(effectContainer:Container, reactionContainer:Container, tie:Tie, reactions:Map[String,Reaction])
        extends StrictLogging {
    val reactionsFields:Set[MonitoredField] =
            reactions.values.flatMap(_.reactionFields.map(MonitoredField(reactionContainer, _) ) ).toSet
    val tieEffectContainerMonitoredFields:Set[MonitoredField] =
            tie.effectContainerMonitoredFields.map( MonitoredField(effectContainer, _) )
    val tieReactionContainerMonitoredFields:Set[MonitoredField] =
            tie.reactionContainerMonitoredFields.map( MonitoredField(reactionContainer, _) )

    val monitoredFields:Set[MonitoredField] =
            reactionsFields ++ tieEffectContainerMonitoredFields ++
            tieReactionContainerMonitoredFields + MonitoredField(effectContainer, "_id")

    def alterWrite(op:Change):Change = op // tie.alterWrite(this, op)
    def fixAll(payloadMongo:MongoClient) {
        logger.trace(s"fixAll $this")
        effectContainer.asLocation.iterator(payloadMongo).foreach( fixOne(payloadMongo, _) )
    }

    def fixOne(payloadMongo:MongoClient, location:ResolvedLocation) {
        logger.trace(s"fixOne $this --- $location")
        val propagated = tie.propagate(this, location)
        val resolved = propagated.resolve(payloadMongo)
        val reactant = resolved.flatMap( _.iterator(payloadMongo) ).map( _.data )
        val effects = reactions.mapValues( _.process(reactant) ).toMap
        location.setValues(payloadMongo, effects)
    }

    // returns (fieldName,expected,got)*
    case class Mismatch(fieldName:String,expected:Option[AnyRef],got:Option[AnyRef])
    def checkOne(payloadMongo:MongoClient, location:ResolvedLocation):Traversable[Mismatch] = {
        val reactant = tie.propagate(this, location).resolve(payloadMongo)
                .flatMap( _.iterator(payloadMongo) ).map( _.data )
        val effects = reactions.mapValues( _.process(reactant) )
        location.iterator(payloadMongo).flatMap { targetDbo =>
            val target = new MongoDBObject(targetDbo.data)
            new MongoDBObject(effects).flatMap{ case (k,v) =>
                val got = target.getAs[AnyRef](k)
                if(got == Option(v) || (got == Some(None) && v == null))
                    None
                else
                    Some(Mismatch(k,Option(v),got))
            }
        }
    }

    def checkAll(payloadMongo:MongoClient):Traversable[(Location,Mismatch)] =
        effectContainer.asLocation.iterator(payloadMongo).flatMap( loc => checkOne(payloadMongo, loc).map( (loc,_) ) )

    def dirtiedSet(op:Change):Set[Location] =
        reactionsFields.flatMap( _.monitor(op) ).flatMap( tie.backPropagate(this, _) ) ++
        tieReactionContainerMonitoredFields.flatMap( _.monitor(op) ).flatMap( tie.backPropagate(this, _) ) ++
        tieEffectContainerMonitoredFields.flatMap( _.monitor(op) ) ++
        MonitoredField(effectContainer, "_id").monitor(op)

    def toJson:JObject = ( effectContainer.toJsonFieldName -> tie.toJson(reactionContainer) ) ~
            reactions.map { case(k,v) => ( k -> v.toJson ) }
}

object Rule {

    def containerFromJson(spec:JValue):Container = spec match {
            case a:JString => a.values.count( _=='.' ) match {
                case 1 => TopLevelContainer(a.values)
                case 2 => NestedContainer(  TopLevelContainer(a.values.split('.').take(2).mkString(".")),
                                            a.values.split('.').last)
            }
            case _ => throw new Error(s"can't parse container spec:" + compact(render(spec)))
        }

    def fromJson(j:JObject):Rule = {
        val (effectContainer,tieSpec,rest):(Container,JValue,JObject)  = {
            val firstKey:String = j.obj.head._1
            if(firstKey == "from")
                throw new Error()
            else {
                (containerFromJson(firstKey), j.obj.head._2, j.obj.drop(1))
            }
        }
        val (tie,reactionContainer):(Tie,Container) = tieSpec match {
            case JString("same") => (SameDocumentTie(), effectContainer)
            case o:JObject => o.obj.head._1 match {
                case "unwind" =>
                    val name:String = o.obj.head._2.asInstanceOf[JString].values
                    (SubDocumentTie(name), NestedContainer(effectContainer.asInstanceOf[TopLevelContainer], name))
                case "follow" =>
                    val follow:String = o.obj.head._2.asInstanceOf[JString].values
                    val to = containerFromJson(JString(o.values("to").asInstanceOf[String]))
                    (FollowKeyTie(follow), to)
                case "search" =>
                    val search:Container = containerFromJson(o.obj.head._2)
                    val by = o.values("by").asInstanceOf[String]
                    (ReverseKeyTie(by), search)
                case _ => throw new Error(s"can't parse tie spec:" + compact(render(tieSpec)))
            }
            case _ => throw new Error(s"can't parse tie spec:" + compact(render(tieSpec)))
        }
        val reactions:Map[String,Reaction] = rest.obj.map { case(name, value) => (name -> (value match {
                case JString(from) => CopyFieldsReaction(from)
                case o:JObject => o.obj.head._1 match {
                    case "mvel" => MVELReaction(
                        o.values("mvel").asInstanceOf[String],
                        o.values.get("using").getOrElse(List()).asInstanceOf[List[String]]
                    )
                    case "class" =>
                        val classLoader = getClass.getClassLoader
                        classLoader.loadClass(o.values("class").asInstanceOf[String]).newInstance.asInstanceOf[Reaction]
                    case "count" => CountReaction()
                }
                case _ => throw new Error(s"can't parse expression: " + compact(render(value)))
            }))
        }.toMap
        Rule(effectContainer, reactionContainer, tie, reactions)
    }
}
// CONTAINERS

@Salat
abstract class Container {
    def asLocation:ResolvedLocation
    def monitor(field:String, op:Change):Set[ResolvedLocation]
    def toLabel:String
    def toJsonFieldName:String
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

    def toJsonFieldName:String = collectionFullName
}

case class NestedContainer(parent:TopLevelContainer, arrayField:String) extends Container {
    def asLocation = NestedSelectorLocation(this, MongoDBObject.empty, AnySubDocumentLocationFilter)
    def monitor(field:String, op:Change) =
        parent.monitor(arrayField, op).map {
            case DocumentLocation(container, doc) => NestedDocumentLocation(this, doc, AnySubDocumentLocationFilter)
            case IdLocation(container, id) => NestedIdLocation(this, id, AnySubDocumentLocationFilter)
            case a => throw new Exception("not implemented")
        } ++ (op match {
            case muc @ ModifiersUpdateChange(writtenCollection, selector, update) =>
                if(muc.impactedFields.contains(arrayField + ".$." + field)) {
                    if(selector.contains(arrayField + "._id") &&
                            SelectorLocation.isAValue(selector.get(arrayField + "._id")))
                        Set(NestedSelectorLocation(this, selector,
                            IdSubDocumentLocationFilter(selector.get(arrayField+"._id"))))
                    else
                        Set(NestedSelectorLocation(this, selector, AnySubDocumentLocationFilter))
                }
                else
                    Set()
            case _ => Set()
        })
    def toLabel = s"<i>$parent.$arrayField</i>"
    def toJsonFieldName:String = parent.collectionFullName + "." + arrayField
}

// TIE

@Salat
abstract class Tie {
    def reactionContainerMonitoredFields:Set[String]
    def effectContainerMonitoredFields:Set[String]

    def propagate(rule:Rule, from:ResolvedLocation):ResolvableLocation
    def backPropagate(rule:Rule, location:ResolvedLocation):Traversable[Location]

    def toLabel:String
    def toJson(reactionContainer:Container):JValue
}

case class SameDocumentTie() extends Tie {
    def reactionContainerMonitoredFields = Set()
    def effectContainerMonitoredFields = Set()

    def propagate(rule:Rule, from:ResolvedLocation):ResolvableLocation = from
    def backPropagate(rule:Rule, location:ResolvedLocation):Traversable[Location] = Some(location)

    def toLabel = "from the same document in"
    def toJson(reactionContainer:Container) = "same"
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
    def toJson(reactionContainer:Container) =
        ( "follow" -> localFieldName) ~
        ( "to" -> reactionContainer.toJsonFieldName)
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
    def toJson(reactionContainer:Container) =
        ( "search" -> reactionContainer.toJsonFieldName) ~
        ( "by" -> reactionFieldName)
}

case class SubDocumentTie(fieldName:String) extends Tie {
    val effectContainerMonitoredFields:Set[String] = Set()
    val reactionContainerMonitoredFields = Set("_id")
    def propagate(rule:Rule, location:ResolvedLocation) = location.asInstanceOf[TopLevelLocation] match {
        case d:DataLocation => NestedDocumentLocation(rule.reactionContainer.asInstanceOf[NestedContainer],
                                                    d.data, AnySubDocumentLocationFilter)
        case id:HaveIdLocation => NestedIdLocation(rule.reactionContainer.asInstanceOf[NestedContainer],
                                                    id.id, AnySubDocumentLocationFilter)
        case tlr:TopLevelResolvedLocation => NestedSelectorLocation(rule.reactionContainer.asInstanceOf[NestedContainer],
                                                    tlr.selector, AnySubDocumentLocationFilter)
    }
    def backPropagate(rule:Rule, location:ResolvedLocation) = location.asInstanceOf[NestedLocation] match {
        case NestedDocumentLocation(_, data, _) => Some(DocumentLocation(rule.effectContainer.asInstanceOf[TopLevelContainer], data))
        case NestedDataDocumentLocation(_, data, _) => Some(DocumentLocation(rule.effectContainer.asInstanceOf[TopLevelContainer], data))
        case NestedIdLocation(_, id, _) => Some(IdLocation(rule.effectContainer.asInstanceOf[TopLevelContainer], id))
        case NestedSelectorLocation(_, sel, _) => Some(SelectorLocation(rule.effectContainer.asInstanceOf[TopLevelContainer], sel))
        case SimpleNestedLocation(_, k, v) => Some(SimpleFilterLocation(rule.effectContainer.asInstanceOf[TopLevelContainer], k, v))
    }
    def toLabel = s"entering <i>$fieldName</i>"
    def toJson(reactionContainer:Container) = ( "unwind" -> fieldName )
}

// REACTION
@Salat abstract class Reaction {
    def reactionFields:Set[String]
    def process(data:Traversable[BSONObject]):Option[AnyRef]
    def toLabel:String
    def toJson:JValue
}

case class CopyFieldsReaction(from:String) extends Reaction {
    val reactionFields:Set[String] = Set(from)
    def process(data:Traversable[BSONObject]) = data.headOption.flatMap(_.getAs[AnyRef](from))
    def toLabel = s"copy <i>$from</i>"
    def toJson = from
}

case class CountReaction() extends Reaction {
    val reactionFields:Set[String] = Set()
    def process(data:Traversable[BSONObject]) = Some(data.size:java.lang.Integer)
    def toLabel = s"count"
    def toJson = ("count" -> true)
}

case class MVELReaction(expr:String, using:List[String]) extends Reaction {
    import org.mvel2.MVEL
    val reactionFields:Set[String] = using.toSet
    val compiled =  MVEL.compileExpression(expr)
    def process(data:Traversable[BSONObject]) = data.headOption.map { doc =>
        try {
            MVEL.executeExpression(compiled, scala.collection.JavaConversions.mapAsJavaMap(doc))
        } catch {
            case a:Throwable =>
                System.err.println(a)
                throw a
        }
    }
    def toLabel = s"normalize <i>$expr</i>"
    def toJson = ("mvel" -> expr) ~ ("using" -> using)
}

object StringNormalizationRule {
    def apply(collection:String, from:String, to:String) =
        Rule(TopLevelContainer(collection), TopLevelContainer(collection), SameDocumentTie(),
                Map(to -> MVELReaction(s"$from.toLowerCase()", List(from))))
}

class InvariantDAO(val db:MongoDB, val lockDuration:FiniteDuration)(implicit ctx: com.novus.salat.Context) {
    val collection = db("invariants")
    val salat = new SalatDAO[Invariant,ObjectId](collection) {}
    val mlp = MongoLockingPool(collection, lockDuration)
}
