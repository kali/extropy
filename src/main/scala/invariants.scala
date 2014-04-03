package org.zoy.kali.extropy

import mongo._

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

abstract class Invariant {
    def monitoredCollections:List[String]
    def alterWrite(op:WriteOp):WriteOp = op match {
        case insert:OpInsert => alterInsert(insert)
    }
    def alterInsert(op:OpInsert):OpInsert
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

    def alterInsert(insert:OpInsert) = insert.copy(
        documents=insert.documents.map { d => d.asInstanceOf[DBObject] ++ ( targetField -> computeOneLocally(d) ) }
    )
}

case class StringNormalizationInvariant[ID](val collection:String, val from:String, val to:String)
        extends SameDocumentInvariant[ID](collection) {
    def pullIdAndSource(id:ID):BSONObject = null
    def sourceFields = Seq(from)
    def targetField = to
    override val computeOneLocally:(BSONObject=>AnyRef) = { d:BSONObject =>
        Option(d.asInstanceOf[DBObject].get(from)).map( _.toString.toLowerCase ).getOrElse(null)
    }
}
