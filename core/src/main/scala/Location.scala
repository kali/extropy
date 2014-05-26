/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._

import mongoutils._

import mongoutils.BSONObjectConversions._

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
sealed trait DataLocation extends ResolvedLocation {
    def data:BSONObject
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
        extends NestedLocation with ResolvedLocation {
    def datas = dbo.getAs[List[BSONObject]](container.arrayField).getOrElse(List()).filter( filter.apply(_) )
    def iterator(payloadMongo:MongoClient) = datas.toTraversable.map{ sub => NestedDataDocumentLocation(container, dbo, sub) }
    def setValues(payloadMongo:MongoClient, values:BSONObject) {
        NestedHelpers.setValues(payloadMongo,container,MongoDBObject("_id" -> dbo.getAs[AnyRef]("_id")),filter,values)
    }
}
case class NestedDataDocumentLocation(container:NestedContainer, dbo:BSONObject, data:BSONObject)
        extends NestedLocation with DataLocation with ResolvedLocation {
    def iterator(payloadMongo:MongoClient) = Traversable(this)
    def setValues(payloadMongo:MongoClient, values:BSONObject) = NestedHelpers.setValues(payloadMongo, container,
        MongoDBObject("_id" -> dbo.getAs[AnyRef]("_id")),IdSubDocumentLocationFilter(data.getAs[AnyRef]("_id")), values)
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
            filter:SubDocumentLocationFilter):Traversable[DataLocation] =
        iteratorOnId(payloadMongo, container, find, filter).map { case(dbo,id) =>
            new NestedDocumentLocation(container, dbo, IdSubDocumentLocationFilter(id)) with DataLocation {
                def data = dbo.getAs[List[BSONObject]](container.arrayField).get.filter( filter ).head
            }
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
    def iterator(payloadMongo:MongoClient):Traversable[DataLocation] =
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

case class NestedSelectorLocation(container:NestedContainer, selector:MongoDBObject, filter:SubDocumentLocationFilter)
        extends NestedLocation with ResolvedLocation {
    val augmentedSelector:MongoDBObject = new MongoDBObject(
        Map(container.arrayField + "._id" -> MongoDBObject("$exists" -> true)) ++ selector
    )
    def iterator(payloadMongo:MongoClient) =
        NestedHelpers.iterator(payloadMongo, container, augmentedSelector, filter)
    def setValues(payloadMongo:MongoClient, values:BSONObject) =
        NestedHelpers.setValues(payloadMongo, container, augmentedSelector, filter, values)
}

