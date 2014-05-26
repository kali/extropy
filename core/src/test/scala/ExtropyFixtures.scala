/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

import org.scalatest._

trait ExtropyFixtures extends BeforeAndAfterAll with MongodbTemporary { this: Suite =>

    val databasesToDiscard = scala.collection.mutable.Buffer[String]()
    def withExtropyAndBlog(testCode:((BaseExtropyContext,BlogFixtures) => Any), loadRules:Boolean=true) {
        val now = System.currentTimeMillis
        val payloadDbName = s"extropy-spec-payload-$now"
        val extropyDbName = s"extropy-spec-internal-$now"
        databasesToDiscard.append( payloadDbName, extropyDbName )
        val fixture = BlogFixtures(payloadDbName)
        val extropy = ExtropyContext(mongoBackendClient(extropyDbName), mongoBackendClient)
        if(loadRules)
            fixture.allRules.foreach( rule => extropy.invariantDAO.salat.insert( Invariant(rule) ) )
        testCode(extropy, fixture)
    }

    override def afterAll {
        databasesToDiscard.foreach( mongoBackendClient.dropDatabase(_) )
        super.afterAll
    }
}
