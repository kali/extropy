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

    def payloadMongo:MongoClient
}

case class Extropy(val extropyDatabase:MongoDB, payloadMongo:MongoClient) extends BaseExtropyContext {
    val agentDAO = new ExtropyAgentDescriptionDAO(extropyDatabase, agentLockDuration)
    val invariantDAO = new InvariantDAO(extropyDatabase, invariantLockDuration)
}

case class DynamicConfiguration(version:Long, invariants:List[Invariant])
object DynamicConfiguration {
    val empty = DynamicConfiguration(-1, List())
}
case class AckDynamicConfiguration(config:DynamicConfiguration)

