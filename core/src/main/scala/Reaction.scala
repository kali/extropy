/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import org.bson.{ BSONObject }
import com.mongodb.casbah.Imports._
import mongoutils.BSONObjectConversions._

import com.novus.salat._
import com.novus.salat.annotations._

abstract class Reaction {
    def reactionFields:Set[String]
    def process(data:Traversable[BSONObject], multiple:Boolean):Option[AnyRef]
    def toLabel:String
    def toMongo:AnyRef
}

object Reaction {
    def fromMongo(data:MongoDBObject) = data.map { case(name, value) => (name -> (value match {
            case from:String => CopyFieldsReaction(from)
            case o:BSONObject => o.keys.head match {
                case "js" => JSReaction(
                    o.get("js").asInstanceOf[String],
                    o.getAs[List[String]]("using").getOrElse(List())
                )
                case "_typeHint" => SalatReaction.fromMongo(o)
            }
            case _ => throw new Error(s"can't parse expression: " + value)
        }))
    }.toMap
}

case class CopyFieldsReaction(from:String) extends Reaction {
    val reactionFields:Set[String] = Set(from)
    def process(data:Traversable[BSONObject], multiple:Boolean) = data.headOption.flatMap(_.getAs[AnyRef](from))
    def toLabel = s"copy <i>$from</i>"
    def toMongo = from
}

object JSReaction {
    import javax.script._
    val engine = new ScriptEngineManager().getEngineByName("nashorn")
}

case class JSReaction(expr:String, using:List[String]=List()) extends Reaction {
    val reactionFields:Set[String] = using.toSet
    import java.util.function.Function
    val function:Function[AnyRef,AnyRef] = {
        import javax.script._
        val jsexpr = "new java.util.function.Function(" + expr +")"
        JSReaction.engine.eval(jsexpr).asInstanceOf[Function[AnyRef,AnyRef]]
    }

    def process(data:Traversable[BSONObject], multiple:Boolean) =
        try {
            import scala.collection.JavaConversions
            if(multiple) {
                Some(function.apply(JavaConversions.asJavaIterable(data.map(_.toMap).toIterable)))
            } else
                data.headOption.map { doc =>
                    function.apply(JavaConversions.mapAsJavaMap(doc))
                }
        } catch {
            case a:Throwable =>
                System.err.println(a)
                throw a
        }
    def toLabel = s"javascript <i>$expr</i>"
    def toMongo = if(using.isEmpty)
        MongoDBObject("js" -> expr)
    else
        MongoDBObject("js" -> expr, "using" -> using)
}

@Salat
abstract class SalatReaction extends Reaction {
    import com.novus.salat.global._
    def toMongo = grater[SalatReaction].asDBObject(this)
}

object SalatReaction {
    import com.novus.salat.global._
    def fromMongo(dbo:DBObject) = grater[SalatReaction].asObject(dbo)
}
