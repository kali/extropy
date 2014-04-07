package org.zoy.kali.extropy

import com.mongodb.casbah.Imports._

object ExtropyContext {
    val hostname = java.net.InetAddress.getLocalHost.getHostName
    val extropyDatabaseName = "extropy"
    val extropyMongoUrl = "mongodb://infrabox:27017"
    val extropyMongoClient = MongoClient(MongoClientURI(extropyMongoUrl))

    val agentDescriptionDAO = new ExtropyAgentDescriptionDAO(extropyMongoClient(extropyDatabaseName))
    val invariantDAO = new InvariantDAO(extropyMongoClient(extropyDatabaseName))
    val configurationVersionDAO = new ConfigurationVersionHolderDAO(extropyMongoClient(extropyDatabaseName))
}

