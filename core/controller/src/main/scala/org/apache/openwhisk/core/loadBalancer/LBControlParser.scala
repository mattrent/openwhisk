package org.apache.openwhisk.core.loadBalancer

import java.util
import java.io.{FileWriter}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

case class InvokerLabel(
  label: String,
  strategy: Option[String],
  maxCapacity: Option[Int],
  maxConcurrentInvocations: Option[Int]
)

sealed trait Invokers
case class InvokerLabels(labels: List[InvokerLabel]) extends Invokers
case class InvokerList(names: List[String]) extends Invokers
//TODO: remove All() => All() is a case of *label, where label is ""
case class All() extends Invokers

sealed trait ControllerSetting
case class ControllerName(name: String) extends ControllerSetting
case class AllControllers() extends ControllerSetting

sealed trait TopologyTolerance
case class AllTolerance() extends TopologyTolerance
case class SameTolerance() extends TopologyTolerance
case class NoneTolerance() extends TopologyTolerance


case class BlockSettings(
  controller: ControllerSetting,
  topology_tolerance: TopologyTolerance,
  strategy: Option[String],
  maxCapacity: Option[Int],
  maxConcurrentInvocations: Option[Int],
  invokersSettings: Invokers
) {
  override def toString: String = s"${controller}, topology_tolerance: ${topology_tolerance}, invokerSettings: ${invokersSettings}, strat: ${strategy}, maxC ${maxCapacity}, maxCI ${maxConcurrentInvocations}"
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

  def logIntoContainer(msg: String) = {
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

  private def parseInvokersSettings(invokerSettings: Option[Any]) : Invokers = {
    logIntoContainer("PARSING INVOKERS")
    logIntoContainer(s"$invokerSettings")

    val invokersList = invokerSettings match {
      case None => All()
      case Some("*") => All()
      case Some(l) =>
        logIntoContainer(s"$l")
        val list = l.asInstanceOf[util.ArrayList[Any]].asScala.toList
        logIntoContainer(s"$list")
        if (list(0).isInstanceOf[String])
          InvokerList(list.asInstanceOf[List[String]])
        else
          InvokerLabels(
            list.map(
              e => {
                val _e = e.asInstanceOf[util.HashMap[String, String]].asScala.toMap
                val _invalidate: Map[String, Int] = parseInvalidate(_e.get("invalidate"))
                InvokerLabel(_e.getOrElse("label", "*"), _e.get("strategy"), _invalidate.get("capacity_used"), _invalidate.get("max_concurrent_invocations"))
              }
            )
          )

    }
    logIntoContainer(s"$invokersList")

    invokersList
  }

  private def parseBlockSettings(blockSettings: Any) : BlockSettings = {
    logIntoContainer("PARSING BLOCK")
    val settings = blockSettings.asInstanceOf[util.HashMap[String, Any]].asScala.toMap

    val controllerName: ControllerSetting = settings.get("controller") match {
      case None => AllControllers()
      case Some("*") => AllControllers()
      case Some(s) => ControllerName(s.toString)
    }

    val toleranceSettings: TopologyTolerance = settings.get("topology_tolerance") match {
      case None => AllTolerance()
      case Some("all") => AllTolerance()
      case Some("none") => NoneTolerance()
      case Some("same") => SameTolerance()
      case _ => AllTolerance()
    }

    val strategy = settings.get("strategy")
    val strategySettings: Option[String] = strategy match {
      case None => None
      case Some(s) => Some(s.toString)
    }

    val invalidate: Map[String, Int] = parseInvalidate(settings.get("invalidate"))

    val invokerSettings = parseInvokersSettings(settings.get("workers"))

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
