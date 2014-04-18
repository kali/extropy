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
case class ModifiersUpdateChange(writtenCollection:String, selector:BSONObject, update:BSONObject) extends Change
case class InsertChange(writtenCollection:String, documents:Stream[BSONObject]) extends Change
case class DeleteChange(writtenCollection:String, selector:BSONObject) extends Change


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

case class MonitoredField(container:Container, field:String)
case class Rule(container:Container, contact:Contact, processor:Processor) {
    def monitoredFields:Set[MonitoredField] = (
        processor.monitoredFields.map( MonitoredField(contact.processorContainer, _ ) )
        ++ contact.localyMonitoredFields.map( MonitoredField( container, _ ) )
        ++ contact.processorMonitoredFields.map( MonitoredField( contact.processorContainer, _ ) )
    )
//    def monitoredCollections:List[String] = contact.monitoredCollections(container)
    def alterWrite(op:Change):Change = op // contact.alterWrite(this, op)
    def activeSync(extropy:BaseExtropyContext) {
        container.iterator(extropy.payloadMongo).foreach { location =>
            val values = processor.process(contact.resolve(location.dbo))
            container.setValues(extropy.payloadMongo, location, values)
        }
    }
}

// CONTAINERS

@Salat
abstract class Container {
    def iterator(payloadMongo:MongoClient):Traversable[Location]
    def collection:String
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject)
}

case class CollectionContainer(collectionFullName:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def collection = collectionFullName
    def iterator(payloadMongo:MongoClient) = {
        val cursor = payloadMongo(dbName)(collectionName).find(MongoDBObject.empty).sort(MongoDBObject("_id" -> 1))
        cursor.option |= com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT
        cursor.toTraversable.map( TopLevelLocation(_) )
    }
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject) {
        payloadMongo(dbName)(collectionName).update(
            MongoDBObject("_id" -> location.dbo.get("_id")),
            MongoDBObject("$set" -> values)
        )
    }
}

case class SubCollectionContainer(collectionFullName:String, arrayField:String) extends Container {
    val dbName = collectionFullName.split('.').head
    val collectionName = collectionFullName.split('.').drop(1).mkString(".")

    def iterator(payloadMongo:MongoClient):Traversable[Location] = null
    def collection:String = collectionFullName
    def setValues(payloadMongo:MongoClient, location:Location, values:MongoDBObject) {}
}

abstract class Location {
    def dbo:DBObject
}

case class TopLevelLocation(dbo:DBObject) extends Location {
}

// CONTACTS

@Salat
abstract class Contact {
    def resolve(from:DBObject):Traversable[DBObject]
    def processorContainer:Container
    def localyMonitoredFields:Set[String]
    def processorMonitoredFields:Set[String]
//    def alterWrite(rule:Rule, change:Change):Change
}

case class SameDocumentContact(container:Container) extends Contact {
    def resolve(from:DBObject) = List(from)
    def processorContainer:Container = container
    def localyMonitoredFields = Set()
    def processorMonitoredFields = Set()
/*
    def alterWrite(rule:Rule, change:Change):Change = change match {
        case insert:InsertChange => insert.copy(
            documents=insert.documents.map { d => MongoDBObject(d.asInstanceOf[DBObject].toList ++
                rule.processor.process(List(d.asInstanceOf[DBObject])).toList) }
        )
        case fbu:FullBodyUpdateChange => fbu.copy(
            update=MongoDBObject(fbu.update.asInstanceOf[DBObject].toList ++
                rule.processor.process(List(fbu.update.asInstanceOf[DBObject])).toList)
        )
        case delete:DeleteChange => delete
        case _ => throw new NotImplementedError
    }
*/
}

case class FollowKeyContact(collectionName:String, localFieldName:String) extends Contact {
    def resolve(from:DBObject) = null
    def processorContainer:Container = CollectionContainer(collectionName)
    val localyMonitoredFields = Set(localFieldName)
    val processorMonitoredFields:Set[String] = Set()
/*
    def alterWrite(rule:Rule, change:Change):Change = change match {
        case insert:InsertChange => insert.copy(
            documents=insert.documents.map { d => MongoDBObject(d.asInstanceOf[DBObject].toList ++
                rule.processor.process(List(d.asInstanceOf[DBObject])).toList) }
        )
        case fbu:FullBodyUpdateChange => fbu.copy(
            update=MongoDBObject(fbu.update.asInstanceOf[DBObject].toList ++
                rule.processor.process(List(fbu.update.asInstanceOf[DBObject])).toList)
        )
        case delete:DeleteChange => delete
        case _ => throw new NotImplementedError
    }
*/
}

case class ReverseKeyContact(container:Container, remoteFieldName:String) extends Contact {
    def resolve(from:DBObject) = null
    def processorContainer:Container = container
    val localyMonitoredFields:Set[String] = Set()
    val processorMonitoredFields = Set(remoteFieldName)
}

// PROCESSORS
@Salat abstract class Processor {
    def monitoredFields:Set[String]
    def process(data:Traversable[DBObject]):DBObject
}

case class CopyField(from:String, to:String)
case class CopyFieldsProcessor(fields:List[CopyField]) extends Processor {
    val monitoredFields:Set[String] = fields.map( _.from ).toSet
    def process(data:Traversable[DBObject]) = MongoDBObject.empty
}

case class CountProcessor(field:String) extends Processor {
    val monitoredFields:Set[String] = Set()
    def process(data:Traversable[DBObject]) = MongoDBObject(field -> data.size)
}

// FOR TESTS

case class StringNormalizationProcessor(from:String, to:String) extends Processor {
    val monitoredFields:Set[String] = Set(from)
    def process(data:Traversable[DBObject]) = Map(to -> (data.headOption match {
        case Some(obj) => obj.get(from).toString.toLowerCase
        case None => null
    }))
}


object StringNormalizationRule {
    def apply(collection:String, from:String, to:String) =
        Rule(CollectionContainer(collection), SameDocumentContact(CollectionContainer(collection)),
                StringNormalizationProcessor(from,to))
}

/*
@Salat
abstract class SameDocumentRule(collection:String) extends Rule {
    val computeOneLocally:(BSONObject=>AnyRef) = null
    val monitoredCollections = List(collection)

    def sourceFields:Seq[String]
    def targetField:String

    def alterWrite(op:Change):Change = op match {
        case insert:InsertChange => insert.copy(
            documents=insert.documents.map { d => d.asInstanceOf[DBObject] ++ ( targetField -> computeOneLocally(d) ) }
        )
        case fbu:FullBodyUpdateChange => fbu.copy(
            update=fbu.update.asInstanceOf[DBObject] ++ (targetField -> computeOneLocally(fbu.update))
        )
        case delete:DeleteChange => delete
        case _ => throw new NotImplementedError
    }

    def activeSync(extropy:BaseExtropyContext) {
        val actualCollection = extropy.payloadMongo(collection.split('.').head)(collection.split('.').drop(1).mkString("."))
        val cursor = actualCollection.find().$orderby( MongoDBObject( "_id" -> 1 ) )
        cursor.option |= com.mongodb.Bytes.QUERYOPTION_NOTIMEOUT
        cursor.foreach { dbo => fixOne(actualCollection, dbo) }
    }

    def fixOne(col:MongoCollection, dbo:BSONObject)
}

@Salat
abstract class ScalarFieldToScalarFieldRule(collection:String, from:String, to:String)
            extends SameDocumentRule(collection) {
    def sourceFields = Seq(from)
    def targetField = to
    override def alterWrite(op:Change):Change = op match {
        case mod:ModifiersUpdateChange =>
            val modifiers = mod.update.asInstanceOf[DBObject]
            val setter = Option(mod.update.asInstanceOf[DBObject].get("$set"))
                .filter( _.isInstanceOf[DBObject] )
                .map( dbo => computeOneLocally(dbo.asInstanceOf[DBObject]) )
            setter match {
                case Some(value) => {
                    val m = mod.copy( update=new MongoDBObject(mod.update.asInstanceOf[DBObject]).clone )
                    m.update.get("$set").asInstanceOf[DBObject].put(to, compute(value))
                    m
                }
                case None => mod
            }
        case op => super.alterWrite(op)
    }
    override val computeOneLocally:(BSONObject=>AnyRef) = { d:BSONObject =>
        Option(d.asInstanceOf[DBObject].get(from)).map( compute(_) ).getOrElse(null)
    }

    def fixOne(col:MongoCollection, dbo:BSONObject) {
        val wanted = computeOneLocally(dbo)
        if(wanted != dbo.get(targetField))
            col.update(MongoDBObject("_id" -> dbo.get("_id")), MongoDBObject("$set" -> MongoDBObject(targetField -> wanted)))
    }

    def compute(src:AnyRef):AnyRef
}

case class StringNormalizationRule(collection:String, from:String, to:String)
        extends ScalarFieldToScalarFieldRule(collection, from, to) {
    override def compute(src:AnyRef):AnyRef = src.toString.toLowerCase
}
*/

class InvariantDAO(val db:MongoDB, val lockDuration:FiniteDuration)(implicit ctx: com.novus.salat.Context) {
    val collection = db("invariants")
    val salat = new SalatDAO[Invariant,ObjectId](collection) {}
    val mlp = MongoLockingPool(collection, lockDuration)
}
