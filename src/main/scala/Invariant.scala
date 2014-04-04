package org.zoy.kali.extropy

import mongo._

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

abstract class Invariant {
    def monitoredCollections:List[String]
    def alterWrite(op:Change):Change
}

abstract class SameDocumentInvariant[ID](collection:String) extends Invariant {
    val computeOneInMongo:(ID=>AnyRef) = null
    val computeOneLocally:(BSONObject=>AnyRef) = null
    val monitoredCollections = List(collection)

    def sourceFields:Seq[String]
    def targetField:String

    def pullIdAndSource(id:ID):BSONObject

    def fixOne(id:ID) {
        val obj:BSONObject = pullIdAndSource(id)
        val value:AnyRef = if(computeOneLocally != null)
                computeOneLocally(obj)
            else
                computeOneInMongo(id)
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

abstract class ScalarFieldToScalarFieldInvariant[ID](collection:String, from:String, to:String)
            extends SameDocumentInvariant[ID](collection) {
    def pullIdAndSource(id:ID):BSONObject = null
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

case class StringNormalizationInvariant[ID](val collection:String, val from:String, val to:String)
        extends ScalarFieldToScalarFieldInvariant[ID](collection, from, to) {
    override def compute(src:AnyRef):AnyRef = src.toString.toLowerCase
}
