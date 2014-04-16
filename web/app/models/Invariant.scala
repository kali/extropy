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

object InvariantPlay extends ModelCompanion[Invariant,ObjectId] {
    val dao = Extropy.invariantDAO.salat

    def command(inv:Invariant, command:InvariantStatus.Value) {
        dao.update(MongoDBObject("_id" -> inv._id), MongoDBObject("$set" -> MongoDBObject("command" -> command.toString)))
    }
}
