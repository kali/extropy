package org.zoy.kali.extropy

import scala.concurrent.duration._

import com.mongodb.casbah.Imports._

trait BaseExtropyContext {
    val hostname = java.net.InetAddress.getLocalHost.getHostName

    def agentHeartBeat = 1 second
    def agentLockDuration = 1 minute

    def overseerHeartBeat = 1 second
    def foremanHeartBeat = 1 second
    def invariantLockDuration = foremanHeartBeat * 10

    def agentDAO:ExtropyAgentDescriptionDAO
    def invariantDAO:InvariantDAO

    def pullConfiguration = DynamicConfiguration(agentDAO.readConfigurationVersion, invariantDAO.all)
}

case class Extropy(val extropyMongoUrl:String, val extropyDatabaseName:String="extropy") extends BaseExtropyContext {
    val extropyMongoClient = MongoClient(MongoClientURI(extropyMongoUrl))
    val agentDAO = new ExtropyAgentDescriptionDAO(extropyMongoClient(extropyDatabaseName), agentLockDuration)
    val invariantDAO = new InvariantDAO(extropyMongoClient(extropyDatabaseName), invariantLockDuration)
}

case class DynamicConfiguration(version:Long, invariants:List[Invariant])
object DynamicConfiguration {
    val empty = DynamicConfiguration(-1, List())
}
case class AckDynamicConfiguration(config:DynamicConfiguration)

