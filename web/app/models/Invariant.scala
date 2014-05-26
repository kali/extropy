/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package models

import play.api.Play.current
import java.util.Date
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import se.radley.plugin.salat._
import salatContext._

import org.zoy.kali.extropy._

object InvariantPlay extends ModelCompanion[Invariant,ObjectId] {
    val dao = Extropy.invariantDAO.salat

    def command(inv:Invariant, command:InvariantStatus.Value) {
        dao.update(MongoDBObject("_id" -> inv._id), MongoDBObject("$set" -> MongoDBObject("command" -> command.toString)))
    }
}
