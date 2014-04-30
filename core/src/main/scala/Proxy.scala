package org.zoy.kali.extropy

case class ExtropyProxy(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration]=None) {

    val configuration = optionalConfiguration.getOrElse(extropy.pullConfiguration)

    // Change -> preChange -> AnalysedChange
/*
    def preChange(originalChange:Change) = AnalysedChange(originalChange,
        configuration.invariants.flatMap{ inv =>
            val d = inv.rule.dirtiedSet(originalChange).map( _.save(extropy) )
            if(!d.isEmpty) Some(inv,d) else None
        }, originalChange)
    case class AnalysedChange(originalChange:Change, dirtiedSet:List[(Invariant, Set[Location])], alteredChange:Change)
*/
    /*
    def syncProcess(originalChange:Change) = {
        val analysed = preChange(originalChange)
    }
    */
}
