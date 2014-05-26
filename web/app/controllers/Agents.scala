/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package controllers

import play.api._
import play.api.mvc._

import com.mongodb.casbah.Imports._

import models.ExtropyAgentDescription

object Agents extends Controller {

    implicit val _menu = Menu("agents")

    def index = Action {
        Ok(views.html.agents(ExtropyAgentDescription.findAll.toList, ExtropyAgentDescription.readConfigurationVersion))
    }

}
