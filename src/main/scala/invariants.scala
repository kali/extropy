package org.zoy.kali.extropy

import org.bson.{ BSONObject }

abstract class Invariant { }

abstract class SameDocumentInvariant[ID] extends Invariant {
    val computeOneInMongo:(ID=>Serializable) = null
    val computeOneLocally:(BSONObject=>Serializable) = null
    def sourceFields:Seq[String]
    def targetField:String

    def pullIdAndSource(id:ID):BSONObject

    def fixOne(id:ID) {
        val obj:BSONObject = pullIdAndSource(id)
        val value:Serializable = if(computeOneLocally != null) computeOneLocally(obj) else computeOneInMongo(id)
    }
}

case class StringNormalizationInvariant[ID](val from:String, val to:String) extends SameDocumentInvariant[ID] {
    def pullIdAndSource(id:ID):BSONObject = null
    def sourceFields = Seq(from)
    def targetField = to
}
