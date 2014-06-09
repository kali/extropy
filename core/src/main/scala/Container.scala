/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import com.mongodb.casbah.Imports._
import mongoutils.BSONObjectConversions._

abstract class Container {
    def asLocation:ResolvedLocation
    def monitor(field:String, op:Change):Set[ResolvedLocation]
    def toLabel:String
    def toMongo:AnyRef
}

object Container {
    def fromMongo(spec:AnyRef):Container = {
        val arraySpec:Seq[String] = spec match {
            case s:String => s.split('.')
            case a:Seq[_] => a.asInstanceOf[Seq[String]]
            case _ => throw new Error(s"can't parse container spec:" + spec)
        }
        arraySpec match {
            case Seq(db,collection) => TopLevelContainer(db,collection)
            case Seq(db,collection,field) => NestedContainer(TopLevelContainer(db,collection), field)
            case _ => throw new Error(s"can't parse container spec:" + spec)
        }
    }
}

case class TopLevelContainer(dbName:String, collectionName:String) extends Container {
    val collectionFullName = dbName + "." + collectionName

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

    def toMongo:AnyRef = if(collectionName.contains('.'))
        List(dbName, collectionName)
    else
        collectionFullName
}

object TopLevelContainer {
    def apply(fullName:String):TopLevelContainer = TopLevelContainer(fullName.split('.').head, fullName.split('.').drop(1).mkString("."))
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
    def toMongo:AnyRef = parent.toMongo match {
        case s:String => s + "." + arrayField
        case l:Seq[_] => l.asInstanceOf[Seq[AnyRef]] ++ Seq(arrayField)
    }
}
