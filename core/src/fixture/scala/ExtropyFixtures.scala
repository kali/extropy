package org.zoy.kali.extropy

import org.scalatest._

trait ExtropyFixtures extends BeforeAndAfterAll with MongodbTemporary { this: Suite =>

    val databasesToDiscard = scala.collection.mutable.Buffer[String]()
    def withExtropyAndBlog(testCode:((BaseExtropyContext,BlogFixtures) => Any)) {
        val now = System.currentTimeMillis
        val payloadDbName = s"extropy-spec-payload-$now"
        val extropyDbName = s"extropy-spec-internal-$now"
        databasesToDiscard.append( payloadDbName, extropyDbName )
        val fixture = BlogFixtures(payloadDbName)
        val extropy = ExtropyContext(mongoBackendClient(extropyDbName), mongoBackendClient)
        fixture.allRules.foreach( rule => extropy.invariantDAO.salat.insert( Invariant(rule) ) )
        testCode(extropy, fixture)
    }

    override def afterAll {
        databasesToDiscard.foreach( mongoBackendClient.dropDatabase(_) )
        super.afterAll
    }
}
