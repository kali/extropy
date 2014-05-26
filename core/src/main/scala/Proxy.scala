/* Copyright © 2014 Mathieu Poumeyrol (kali at zoy dot org).
 * This work is free. You can redistribute it and/or modify it under the
 * terms of the Do What The Fuck You Want To Public License, Version 2,
 * as published by Sam Hocevar. See the COPYING file for more details.
 */
package org.zoy.kali.extropy

case class AnalysedChange(originalChange:Change, dirtiedSet:List[(Invariant, Set[Location])], alteredChange:Change)
case class ResolvableChange(originalChange:Change, dirtiedSet:List[(Invariant, Set[ResolvableLocation])], alteredChange:Change)

case class ExtropyProxyLogic(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration]=None) {

    val configuration = optionalConfiguration.getOrElse(extropy.pullConfiguration)

    // Change -> analyse -> AnalysedChange
    def analyse(originalChange:Change) = AnalysedChange(originalChange,
        configuration.invariants.map{ inv => (inv, inv.rule.dirtiedSet(originalChange) ) }, originalChange)

    // AnalysedChange -> snapshot -> ResolvableChange
    def save(change:AnalysedChange):ResolvableChange = {
        val snapped = change.dirtiedSet.map { set => ( set._1, set._2.flatMap {
            case a:ShakyLocation => a.save(extropy.payloadMongo)
            case a:ResolvableLocation => Traversable(a)
        } ) }
        ResolvableChange(change.originalChange, snapped, change.alteredChange)
    }

    def preChange(originalChange:Change) = save(analyse(originalChange))

    def postChange(change:ResolvableChange) {
        change.dirtiedSet.foreach { case(inv, locations) =>
            locations.flatMap( _.resolve(extropy.payloadMongo) ).foreach( inv.rule.fixOne(extropy.payloadMongo, _ ) )
        }
    }
}

case class SyncProxy(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration] = None) {

    val logic = ExtropyProxyLogic(extropy, optionalConfiguration)

    def doChange(change:Change) = {
        val processedChange = logic.preChange(change)
        val result = processedChange.alteredChange.play(extropy.payloadMongo)
        logic.postChange(processedChange)
        result
    }

}
