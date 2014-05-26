/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package controllers

import play.api._
import play.api.mvc._

import com.mongodb.casbah.Imports._

import models._
import org.zoy.kali.extropy.{ Invariant, InvariantStatus }

object Invariants extends Controller {

    implicit val _menu = Menu("invariants")

    import models.Extropy._

    def index = Action { req =>
        implicit val rh:RequestHeader = req
        Ok(views.html.invariants(InvariantPlay.findAll.toList))
    }

    def command(id:ObjectId, command:String) = Action {
        val inv = InvariantPlay.findOneById(id).get
        InvariantPlay.command(inv, InvariantStatus.withName(command))
        Ok
    }
}
