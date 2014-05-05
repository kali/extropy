package org.zoy.kali.extropy

case class AnalysedChange(originalChange:Change, dirtiedSet:List[(Invariant, Set[Location])], alteredChange:Change)

case class ExtropyProxyLogic(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration]=None) {

    val configuration = optionalConfiguration.getOrElse(extropy.pullConfiguration)

    // Change -> analyse -> AnalysedChange
    def analyse(originalChange:Change) = AnalysedChange(originalChange,
        configuration.invariants.map{ inv => (inv, inv.rule.dirtiedSet(originalChange) ) }, originalChange)

    // AnalysedChange -> snapshot -> AnalysedChange
    def snapshot(change:AnalysedChange):AnalysedChange = {
        val snapped = change.dirtiedSet.map( set => ( set._1, set._2.flatMap( _.snapshot(extropy.payloadMongo) ) ) )
        AnalysedChange(change.originalChange, snapped, change.alteredChange)
    }

    def preChange(originalChange:Change) = snapshot(analyse(originalChange))

    def postChange(change:AnalysedChange) {
        change.dirtiedSet.foreach { case(inv, locations) => locations.flatMap( _.expand(extropy.payloadMongo) ).foreach( inv.rule.fixOne(extropy.payloadMongo, _ ) ) }
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
