package org.zoy.kali.extropy

case class ExtropyProxy(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration]=None) {

    val configuration = optionalConfiguration.getOrElse(extropy.pullConfiguration)

    // Change -> preChange -> AnalysedChange
    def preChange(originalChange:Change) = AnalysedChange(originalChange,
        configuration.invariants.flatMap{ inv =>
            inv.rule.dirtiedSet(originalChange).map( loc => (inv, loc.expand(extropy.payloadMongo).toSet) )
        }, originalChange)
    case class AnalysedChange(originalChange:Change, dirtiedSet:List[(Invariant, Set[Location])], alteredChange:Change)

    /*
    def syncProcess(originalChange:Change) = {
        val analysed = preChange(originalChange)
    }
    */
}

