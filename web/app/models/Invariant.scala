package models

import play.api.Play.current
import java.util.Date
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import se.radley.plugin.salat._
import salatContext._

case class Invariant(   _id:ObjectId, /* rule:Rule, */ emlp:MongoLock, statusChanging:Boolean=false,
                        status:InvariantStatus.Value=InvariantStatus.Created,
                        command:Option[InvariantStatus.Value])

object InvariantStatus extends Enumeration {
    val Created = Value("created")
    val Stop = Value("stop")                // nobody does nothing. sync will be required
    val Sync = Value("sync")                // all proxies are "sync", foreman syncs actively
    val Run = Value("run")                  // all proxies are "run"
    val Error = Value("error")
}

object Invariant extends ModelCompanion[Invariant,ObjectId] {
    val dao = new SalatDAO[Invariant,ObjectId](collection=mongoCollection("invariants")) {}

    def command(inv:Invariant, command:InvariantStatus.Value) {
        dao.update(MongoDBObject("_id" -> inv._id), MongoDBObject("$set" -> MongoDBObject("command" -> command.toString)))
    }
}
