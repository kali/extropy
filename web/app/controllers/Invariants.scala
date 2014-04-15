package controllers

import play.api._
import play.api.mvc._

import com.mongodb.casbah.Imports._

import models.{ Invariant, InvariantStatus }

object Invariants extends Controller {

    implicit val _menu = Menu("invariants")

    def index = Action { req =>
        implicit val rh:RequestHeader = req
        Ok(views.html.invariants(Invariant.findAll.toList))
    }

    def command(id:ObjectId, command:String) = Action {
        val inv = Invariant.findOneByID(id).get
        Invariant.command(inv, InvariantStatus.withName(command))
        Ok
    }
}
