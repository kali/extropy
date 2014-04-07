package org.zoy.kali.extropy

import com.mongodb.casbah.Imports._

object ExtropyContext {
    val hostname = java.net.InetAddress.getLocalHost.getHostName
    val extropyDatabaseName = "extropy"
    lazy val extropyMongoUrl = "mongodb://infrabox:27017"
    lazy val extropyMongoClient = MongoClient(MongoClientURI(extropyMongoUrl))
    lazy val agentDescriptionDAO = new ExtropyAgentDescriptionDAO(extropyMongoClient(extropyDatabaseName))
}

