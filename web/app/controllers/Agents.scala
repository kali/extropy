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
