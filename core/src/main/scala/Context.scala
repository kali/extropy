package org.zoy.kali.extropy

import com.mongodb.casbah.Imports._

trait BaseExtropyContext {
    val hostname = java.net.InetAddress.getLocalHost.getHostName
    val extropyDatabaseName = "extropy"
    def extropyMongoUrl:String
    val extropyMongoClient = MongoClient(MongoClientURI(extropyMongoUrl))

    val agentDAO = new ExtropyAgentDescriptionDAO(extropyMongoClient(extropyDatabaseName))
    val invariantDAO = new InvariantDAO(extropyMongoClient(extropyDatabaseName))
}

object ExtropyContext extends BaseExtropyContext {
    def extropyMongoUrl = "mongodb://infrabox:27017"
}
