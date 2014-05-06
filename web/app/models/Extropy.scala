package models

import org.zoy.kali.extropy._
import se.radley.plugin.salat._

import salatContext._

object Extropy extends BaseExtropyContext {
    val salatPlugin = play.api.Play.current.plugin[SalatPlugin].get

    val extropyDatabase = salatPlugin.source("extropy").db
    val payloadMongo = salatPlugin.source("payload").connection
    val agentDAO = new ExtropyAgentDescriptionDAO(extropyDatabase, agentLockDuration)
    val invariantDAO = new InvariantDAO(extropyDatabase, invariantLockDuration)
}

/*
% MONGO_FOR_TEST=localhost:27017 ./sbt
[...]
test:console
[...]
val client = MongoClient()
val extropy = ExtropyContext(client("extropy"), client)
BlogFixtures("blog").allRules.foreach( r => extropy.invariantDAO.salat.insert(Invariant(r)) )
*/
