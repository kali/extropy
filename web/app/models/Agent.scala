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

object ExtropyAgentDescription extends ModelCompanion[ExtropyAgentDescription,String] {
    val dao = new SalatDAO[ExtropyAgentDescription,String](collection=mongoCollection("agents")) {}

    def readConfigurationVersion:Long = mongoCollection("configuration_version").findOne(MongoDBObject("_id" -> "version"))
                        .flatMap( d => d.getAs[Long]("value") ).getOrElse(0L)
}
