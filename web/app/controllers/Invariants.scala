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
        val inv = InvariantPlay.findOneByID(id).get
        InvariantPlay.command(inv, InvariantStatus.withName(command))
        Ok
    }
}
