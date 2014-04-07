package controllers

import play.api._
import play.api.mvc._

import com.mongodb.casbah.Imports._

import models.ExtropyAgentDescription

object Agents extends Controller {

    def index = Action {
        Ok(views.html.agents(ExtropyAgentDescription.findAll.toList))
    }

}
