package controllers

import models.Task

import play.api._
import play.api.mvc._

import play.api.data._
import play.api.data.Forms._

import com.mongodb.casbah.Imports._

object Application extends Controller {

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