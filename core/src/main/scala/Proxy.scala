package org.zoy.kali.extropy

case class ExtropyProxy(extropy:BaseExtropyContext, optionalConfiguration:Option[DynamicConfiguration]=None) {

    val configuration = optionalConfiguration.getOrElse(extropy.pullConfiguration)

    def processChange(originalChange:Change):Change = {
        configuration.invariants.foldLeft(originalChange) { (change,inv) =>
            if(inv.rule.monitoredCollections.contains(change.writtenCollection))
                inv.rule.alterWrite(change)
            else
                change
        }
    }
}
