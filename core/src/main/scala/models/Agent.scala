package org.zoy.kali.extropy.models

import java.util.Date
import com.novus.salat.annotations._

case class MongoLock(@Key("lu") until:Date, @Key("lb") locker:Option[AnyRef])
case class ExtropyAgentDescription(_id:String, emlp:MongoLock, configurationVersion:Long=(-1L))

