package models

import org.zoy.kali.extropy.models._

import play.api.Play.current
import java.util.Date
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.dao._
import com.mongodb.casbah.Imports._
import se.radley.plugin.salat._
import salatContext._

object ExtropyAgentDescription extends ModelCompanion[ExtropyAgentDescription,String] {
    val dao = new SalatDAO[ExtropyAgentDescription,String](collection=mongoCollection("agents")) {}
}
