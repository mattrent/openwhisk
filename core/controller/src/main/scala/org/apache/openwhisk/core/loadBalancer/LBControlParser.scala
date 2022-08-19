package org.apache.openwhisk.core.loadBalancer

import java.util
import java.io.{FileWriter}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

case class WorkerLabel(
  label: String,
  strategy: Option[String],
  maxCapacity: Option[Int],
  maxConcurrentInvocations: Option[Int]
)

case class WorkerName(
  label: String,
  maxCapacity: Option[Int],
  maxConcurrentInvocations: Option[Int]
)

sealed trait Workers
case class WorkerSet(labels: List[WorkerLabel]) extends Workers
case class WorkerList(names: List[WorkerName]) extends Workers
case class All() extends Workers

sealed trait ControllerSettings
case class ControllerName(name: String) extends ControllerSettings
case class AllControllers() extends ControllerSettings

sealed trait TopologyTolerance
case class AllTolerance() extends TopologyTolerance
case class SameTolerance() extends TopologyTolerance
case class NoneTolerance() extends TopologyTolerance

/* TODO:
    - no more "block" keyword
    - "wrk:" and "set:" for single/multiple workers
    - no more "*" for all workers (now null is the "all" value)
    - no more "*" for all controllers (omitting the "controller" value sets it as "all")
    - tags are now a list

   BlockSettings pretty much the same as before (doesn't need "blocks" keyword, simply a list)

   The returned object must be a map; since tags are a list now, it must be converted from [{"a":{}}, {"b":{}}] to {"a":{}, "b":{}}
 */
case class BlockSettings(
  controller: ControllerSettings,
  topology_tolerance: TopologyTolerance,
  strategy: Option[String],
  maxCapacity: Option[Int],
  maxConcurrentInvocations: Option[Int],
  workersSettings: Workers
) {
  override def toString: String = s"${controller}, topology_tolerance: ${topology_tolerance}, invokerSettings: ${workersSettings}, strat: ${strategy}, maxC ${maxCapacity}, maxCI ${maxConcurrentInvocations}"
}

// Oggetto per ogni function tag che contiene tutte le impostazioni
case class TagSettings(tag: String,
                       blockSettings: List[BlockSettings],
                       followUp : Option[String],
                       strategy : Option[String])
{
  // override def toString: String = s"Tag Settings for tag: $tag. With ${blockSettings.length} blocks, ${strategy} strategy and ${followUp} followup. ${blockSettings}"
  override def toString: String = s"Tag Settings for tag: $tag. With ${strategy} strategy and ${followUp} followup. ${blockSettings}"
}

// Oggetto per tutte le impostazioni di un file smart lb config yml
case class ConfigurableLBSettings(settings : Map[String, TagSettings]) {
  // Dovrebbe avere una mappa tag -> invokerList
  // Si perchè al momento andrebbe a fare una roba tipo tagsettings filter tag. Invece è meglio avere tag -> tagsettings

  def getTagSettings(tag : String): Option[TagSettings] = settings.get(tag)

}


object LBControlParser {

  def logIntoContainer(msg: String): Any = {
    val fw = new FileWriter("parserLogs.txt", true)
    try {
      fw.append(s"$msg\n")
      }
    catch {
      case e: Throwable => println(e)
    }
    finally {
      fw.close()
    }
  }

  private def parseInvalidate(invalidateSettings: Option[Any]): Map[String, Int] = {
    invalidateSettings match {
      case Some("overload") => Map()
      case l => l.asInstanceOf[Option[util.ArrayList[util.HashMap[String, Int]]]] match {
        case None => Map()
        case Some(l) => l.asScala.toList.flatMap(m => m.asScala.toMap).toMap
      }
    }
  }

  private def parseWorkersSettings(workerSettings: Option[Any]) : Workers = {
    logIntoContainer("PARSING INVOKERS")
    logIntoContainer(s"$workerSettings")

    val invokersList = workerSettings match {
      case None => throw new Exception("Invalid configuration file: mandatory workers key missing")
      case Some(settingsList) =>
        logIntoContainer(s"$settingsList")
        val list = settingsList.asInstanceOf[util.ArrayList[Map[Any, Any]]].asScala.toList
        logIntoContainer(s"$list")
        val first = list.head
        if (first.contains("wrk")) {
//          TODO: parse worker list and worker set
          WorkerList(list.asInstanceOf[List[String]])
        }
        else if (first.contains("set")) {
            WorkerLabel(
              list.map(
                e => {
                  val _e = e.asInstanceOf[util.HashMap[String, String]].asScala.toMap
                  val _invalidate: Map[String, Int] = parseInvalidate(_e.get("invalidate"))
                  WorkerLabel(_e.getOrElse("label", "*"), _e.get("strategy"), _invalidate.get("capacity_used"), _invalidate.get("max_concurrent_invocations"))
                }
              )
            )
          }
        else throw new RuntimeException("Invalid configuration file: mandatory wrk or set keys")

    }
    logIntoContainer(s"$invokersList")

    invokersList
  }

  private def parseBlockSettings(blockSettings: Any) : BlockSettings = {
    logIntoContainer("PARSING BLOCK")
    val settings = blockSettings.asInstanceOf[util.HashMap[String, Any]].asScala.toMap

    val controllerName: ControllerSettings = settings.get("controller") match {
      case None => AllControllers()
      case Some(s) => ControllerName(s.toString)
    }

    val toleranceSettings: TopologyTolerance = if (controllerName != AllControllers()) {
      settings.get("topology_tolerance") match {
        case Some("all") => AllTolerance()
        case Some("none") => NoneTolerance()
        case Some("same") => SameTolerance()
        case _ => AllTolerance()
      }
    } else {
      AllTolerance()
    }

    val strategy = settings.get("strategy")
    val strategySettings: Option[String] = strategy match {
      case None => None
      case Some(s) => Some(s.toString)
    }

    val invalidate: Map[String, Int] = parseInvalidate(settings.get("invalidate"))

    val invokerSettings = parseWorkersSettings(settings.get("workers"))

    BlockSettings(controllerName, toleranceSettings, strategySettings, invalidate.get("capacity_used"), invalidate.get("max_concurrent_invocations"), invokerSettings)
  }

  private def parseTagSettings(tag : (String, Any)) : (String, TagSettings) = {
    val tagName = tag._1
    logIntoContainer(s"PARSING TAG $tagName")

    val settings: Map[String, Any] = tag._2.asInstanceOf[util.HashMap[String, Any]].asScala.toMap

    logIntoContainer(s"${tag._2}")

    val blocks: List[Any] = settings.get("blocks") match {
      case None => List[Any]()
      case Some(b) => b.asInstanceOf[util.ArrayList[Any]].asScala.toList
    }
    val blockSettings: List[BlockSettings] = blocks.map(parseBlockSettings)

    val followUp = settings.get("followup")
    val strategy = settings.get("strategy")

    val followUpSettings: Option[String] = followUp match {
      case None => None
      case Some(s) => Some(s.toString)
    }

    val strategySettings: Option[String] = strategy match {
      case None => None
      case Some(s) => Some(s.toString)
    }

    blockSettings.foreach(b => logIntoContainer(s"$b"))
    logIntoContainer(s"$followUp")
    logIntoContainer(s"$strategy")

    tagName -> TagSettings(tagName, blockSettings, followUpSettings, strategySettings)
  }

  def parseConfigurableLBSettings(configurationYAMLText : String) : ConfigurableLBSettings = {
    val parsedYaml: Map[String, Any] = new Yaml().load[util.HashMap[String, Any]](configurationYAMLText).asScala.toMap

    logIntoContainer(parsedYaml.toString)

    ConfigurableLBSettings(parsedYaml.map(parseTagSettings))
  }

}
