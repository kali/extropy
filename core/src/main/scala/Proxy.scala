package org.zoy.kali.extropy

abstract class ExtropyProxy(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration]=None) {

    val configuration = optionalConfiguration.getOrElse(extropy.pullConfiguration)

    // Change -> preChange -> AnalysedChange
    def preChange(originalChange:Change) = AnalysedChange(originalChange,
        configuration.invariants.map{ inv => (inv, inv.rule.dirtiedSet(originalChange) ) }, originalChange)
    case class AnalysedChange(originalChange:Change, dirtiedSet:List[(Invariant, Set[Location])], alteredChange:Change)

    // AnalysedChange -> snapshot -> AnalysedChange
    def snapshot(change:AnalysedChange):AnalysedChange = {
        val snapped = change.dirtiedSet.map( set => ( set._1, set._2.flatMap( _.snapshot(extropy.payloadMongo) ) ) )
        AnalysedChange(change.originalChange, snapped, change.alteredChange)
    }

    /*
    def syncProcess(originalChange:Change) = {
        val analysed = preChange(originalChange)
    }
    */
}

case class SyncProxy(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration] = None)
    extends ExtropyProxy(extropy, optionalConfiguration) {

    def doChange(change:Change) {
        val analysed = preChange(change)
        val snapped = snapshot(analysed)
        val result = snapped.alteredChange.play(extropy.payloadMongo)
        result
    }

}
