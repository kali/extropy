/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

import com.novus.salat.dao._

import mongoutils.BSONObjectConversions._

import scala.concurrent.duration.FiniteDuration

import mongoutils._

import com.novus.salat.transformers.CustomTransformer

import com.typesafe.scalalogging.slf4j.StrictLogging

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
                        command:Option[InvariantStatus.Value]=None) {
}

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

object RuleCodec extends CustomTransformer[Rule, DBObject] {
    def deserialize(o:DBObject) = Rule.fromMongo(o)
    def serialize(r:Rule) = r.toMongo
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
        val effects = reactions.mapValues( _.process(reactant, tie.propageToMultiple) ).toMap
        location.setValues(payloadMongo, effects)
    }

    // returns (fieldName,expected,got)*
    case class Mismatch(fieldName:String,expected:Option[AnyRef],got:Option[AnyRef])
    def checkOne(payloadMongo:MongoClient, location:ResolvedLocation):Traversable[Mismatch] = {
        val reactant = tie.propagate(this, location).resolve(payloadMongo)
                .flatMap( _.iterator(payloadMongo) ).map( _.data )
        val effects = reactions.mapValues( _.process(reactant, tie.propageToMultiple) )
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

    def toMongo = {
        val rule:MongoDBObject = MongoDBObject("from" -> effectContainer.toMongo) ++ MongoDBObject(tie.toMongo(reactionContainer).toList)
        MongoDBObject("rule" -> rule) ++ MongoDBObject(reactions.mapValues { _.toMongo }.toList)
    }
}

object Rule {

    def fromMongo(j:MongoDBObject):Rule = {
        val (effectContainer,tieSpec):(Container,MongoDBObject)  = {
            val rule:MongoDBObject = j.getAs[BSONObject]("rule").get
            (Container.fromMongo(rule("from")), rule - "from")
        }
        val (tie,reactionContainer):(Tie,Container) = Tie.fromMongo(tieSpec, effectContainer)
        val reactions:Map[String,Reaction] = Reaction.fromMongo(j-"rule")
        Rule(effectContainer, reactionContainer, tie, reactions)
    }
}

class InvariantDAO(val db:MongoDB, val lockDuration:FiniteDuration)(implicit ctx: com.novus.salat.Context) {
    val serializer = RuleCodec
    val collection = db("invariants")
    val salat = new SalatDAO[Invariant,ObjectId](collection) {}
    val mlp = MongoLockingPool(collection, lockDuration)
}
