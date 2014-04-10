package org.zoy.kali.extropy

import mongo._

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.novus.salat.global._

import scala.concurrent.duration._

import MongoLockingPool.LockerIdentity

@Salat
abstract class Invariant {
    def id:ObjectId
    def monitoredCollections:List[String]
    def alterWrite(op:Change):Change
}

abstract class SameDocumentInvariant(collection:String) extends Invariant {
    val computeOneInMongo:(AnyRef=>AnyRef) = null
    val computeOneLocally:(BSONObject=>AnyRef) = null
    val monitoredCollections = List(collection)

    def sourceFields:Seq[String]
    def targetField:String

    def pullIdAndSource(id:AnyRef):BSONObject

    def fixOne(id:AnyRef) {
        val obj:BSONObject = pullIdAndSource(id)
        val value:AnyRef = if(computeOneLocally != null)
                computeOneLocally(obj)
            else
                computeOneInMongo(id)
        obj.put(targetField, value)
    }

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
}

abstract class ScalarFieldToScalarFieldInvariant(collection:String, from:String, to:String)
            extends SameDocumentInvariant(collection) {
    def pullIdAndSource(id:AnyRef):BSONObject = null
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
    def compute(src:AnyRef):AnyRef
}

case class StringNormalizationInvariant(@Key("_id") id:ObjectId, collection:String, from:String, to:String)
        extends ScalarFieldToScalarFieldInvariant(collection, from, to) {
    override def compute(src:AnyRef):AnyRef = src.toString.toLowerCase
}

class InvariantDAO(val db:MongoDB, val lockDuration:FiniteDuration) {
    val collection = db("invariants")
    val salat = new SalatDAO[Invariant,ObjectId](collection) {}
    val mlp = MongoLockingPool(collection, lockDuration)

    def all = salat.find(MongoDBObject.empty).toList

    def prospect(implicit by:LockerIdentity):Option[Invariant] =
        mlp.lockOne().map( salat._grater.asObject(_) )
    def claim(invariant:Invariant)(implicit by:LockerIdentity):Invariant =
        salat._grater.asObject(mlp.relock(salat._grater.asDBObject(invariant)))
}
