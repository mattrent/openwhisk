package org.apache.openwhisk.core.loadBalancer

import java.util
import java.io.{FileWriter}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._


sealed trait WorkerLabel

case class WorkerAll() extends WorkerLabel
case class WorkerSubset(s: String) extends WorkerLabel

case class WorkerSet(
  label: WorkerLabel,
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
case class WorkerSetList(labels: List[WorkerSet]) extends Workers
case class WorkerList(names: List[WorkerName]) extends Workers

sealed trait ControllerSettings
case class ControllerName(name: String) extends ControllerSettings
case class AllControllers() extends ControllerSettings

sealed trait TopologyTolerance
case class AllTolerance() extends TopologyTolerance
case class SameTolerance() extends TopologyTolerance
case class NoneTolerance() extends TopologyTolerance

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
        val list = settingsList.asInstanceOf[util.ArrayList[util.HashMap[Any, Any]]].asScala.toList
        logIntoContainer(s"$list")
        val first = list.head.asScala.toMap
        if (first.contains("wrk")) {
          // we are scanning a worker name list
          val names = list.map(rawElement => {
            val element = rawElement.asScala.toMap
            val _invalidate = parseInvalidate(element.get("invalidate"))
            WorkerName(element.get("wrk").get.toString, _invalidate.get("capacity_used"), _invalidate.get("max_concurrent_invocations"))
          })
          WorkerList(names)
        }
        else if (first.contains("set")) {
          // we are scanning a worker label set
          val names = list.map(rawElement => {
            val element = rawElement.asScala.toMap
            val _invalidate = parseInvalidate(element.get("invalidate"))
            val _strategy = element.get("strategy").map(_.toString)
            val _set = element.get("set") match {
              case Some(null) => WorkerAll()
              case Some(s) => WorkerSubset(s.toString)
              case None => WorkerAll()
            }
            WorkerSet(_set, _strategy , _invalidate.get("capacity_used"), _invalidate.get("max_concurrent_invocations"))
          })
          WorkerSetList(names)
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

  private def parseTagSettings(tag : (String, Map[String, Any])) : (String, TagSettings) = {
    val settings: Map[String, Any] = tag._2
    val tagName = tag._1
    logIntoContainer(s"PARSING TAG $tagName")

    logIntoContainer(s"${tag._2}")

    val blocks: List[Any] = settings.get("blocks").get match {
      case None => List[Any]()
      case Some(b) => b.asInstanceOf[util.ArrayList[Any]].asScala.toList
    }
    val blockSettings: List[BlockSettings] = blocks.map(parseBlockSettings)

    val followUp = settings.get("followup").get
    val strategy = settings.get("strategy").get

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
    /*
    The base YAML produces the following shape:
    [{"tagName": [], "strategy": _, "followup": _}, ...]

    The object is then mapped to:
    {"tagName": {"blocks":[], "strategy": _, "followup": _}, ...}
     */
    val baseParsedYaml: util.ArrayList[util.HashMap[String, Any]] = new Yaml().load[
      util.ArrayList[util.HashMap[String, Any]]
    ](configurationYAMLText)

    val parsedYaml: Map[String, Map[String, Any]] = baseParsedYaml.asScala.toList.map(rawElement => {
      val element = rawElement.asScala.toMap
      // the tag name is the only key that is both not "strategy" nor "followup"
      val tagName: String = element.keys.filter(k => k != "strategy" && k != "followup").head
      // the block list is the array identified by the tagName key
      val innerMap: Map[String, Any] = Map.apply(
        ("strategy", element.get("strategy")),
        ("followup", element.get("followup")),
        ("blocks", element.get(tagName))
      )
      tagName -> innerMap
    }).toMap

    logIntoContainer(s"Base parsed YAML: ${baseParsedYaml.toString}")
    logIntoContainer(s"Parsed YAML: ${parsedYaml.toString}")

    ConfigurableLBSettings(parsedYaml.map(parseTagSettings))
  }

}
