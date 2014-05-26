/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package controllers

import models.Task

import play.api._
import play.api.mvc._

import play.api.data._
import play.api.data.Forms._

import com.mongodb.casbah.Imports._

import se.radley.plugin.salat._

case class MenuItem(id:String, label:String, route:Call)
case class Menu(active:String) {
    val items = List(
        MenuItem("agents", "Agents", routes.Agents.index),
        MenuItem("invariants", "Invariants", routes.Invariants.index)
    )
}

object Application extends Controller {

    implicit val _menu = Menu("")

    val taskForm = Form(
      "label" -> nonEmptyText
    )

    def index = Action {
        Redirect(routes.Application.tasks)
    }

    def tasks = Action {
        Ok(views.html.index(Task.findAll.toList, taskForm))
    }

    def newTask = Action { implicit request =>
        taskForm.bindFromRequest.fold(
            errors => BadRequest(views.html.index(Task.findAll.toList, errors)),
            label => {
                Task.save(Task(new ObjectId, label))
                Redirect(routes.Application.tasks)
            }
        )
    }

    def deleteTask(id:ObjectId) = Action {
        Task.removeById(id)
        Redirect(routes.Application.tasks)
    }
}
