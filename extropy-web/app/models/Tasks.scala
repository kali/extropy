package models

import play.api.Play.current
import java.util.Date
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import se.radley.plugin.salat._
import salatContext._

case class Task(id:ObjectId, label:String)

object Task extends ModelCompanion[Task, ObjectId] {
  val dao = new SalatDAO[Task, ObjectId](collection = mongoCollection("tasks")) {}
}
