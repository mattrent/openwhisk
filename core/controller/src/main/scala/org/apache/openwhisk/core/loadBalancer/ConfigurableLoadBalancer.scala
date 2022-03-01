package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.management.scaladsl.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.apache.kafka.clients.producer.RecordMetadata
import pureconfig._
import pureconfig.generic.auto._
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.core.controller.{Controller, RejectRequest}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader

import java.io.FileNotFoundException
import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.{FiniteDuration}
import scala.io.Source
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException


class ConfigurableLoadBalancer(
                                config: WhiskConfig,
                                implicit val controllerInstance: ControllerInstanceId,
                                feedFactory: FeedFactory,
                                val invokerPoolFactory: InvokerPoolFactory,
                                implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
                                implicit actorSystem: ActorSystem,
                                logging: Logging,
                                materializer: ActorMaterializer)
  extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

    logging.info(this, s"Load balancer on controller with nodeName: ${controllerInstance.controllerNodeName}")

    /** Build a cluster of all loadbalancers */
    private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
        AkkaManagement(actorSystem).start()
        ClusterBootstrap(actorSystem).start()
        Some(Cluster(actorSystem))
    } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
        Some(Cluster(actorSystem))
    } else {
        None
    }

    /** State needed for scheduling. */
    val schedulingState: ConfigurableLoadBalancerState = ConfigurableLoadBalancerState()(lbConfig)

    /**
     * Monitors invoker supervision and the cluster to update the state sequentially
     *
     * All state updates should go through this actor to guarantee that
     * [[ConfigurableLoadBalancerState.updateInvokers]] and [[ConfigurableLoadBalancerState.updateCluster]]
     * are called exclusive of each other and not concurrently.
     */

    val mediator = DistributedPubSub(actorSystem).mediator

    private val monitor = actorSystem.actorOf(Props(new Actor {
        override def preStart(): Unit = {
            cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
            mediator ! Subscribe("refresh", self)
            mediator ! Subscribe("invocation", self)
        }

        // all members of the cluster that are available
        var availableMembers = Set.empty[Member]

        override def receive: Receive = {
            case CurrentInvokerPoolState(newState) =>
                schedulingState.updateInvokers(newState)

            // State of the cluster as it is right now
            case CurrentClusterState(members, _, _, _, _) =>
                availableMembers = members.filter(_.status == MemberStatus.Up)
                schedulingState.updateCluster(availableMembers.size)

            // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
            // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
            // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
            // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
            // going into overflow mode prematurely.
            case event: ClusterDomainEvent =>
                availableMembers = event match {
                    case MemberUp(member)          => availableMembers + member
                    case ReachableMember(member)   => availableMembers + member
                    case MemberRemoved(member, _)  => availableMembers - member
                    case UnreachableMember(member) => availableMembers - member
                    case _                         => availableMembers
                }
                schedulingState.updateCluster(availableMembers.size)


            case ConfigurationUpdated(newConfig) =>
                logging.info(this, "ConfigurableLB: Configuration update request received.")
                schedulingState.updateConfigurableLBSettings(newConfig)

            case PropagateInvocation(serializedAction, serializedMsg, serializedTransid) =>
                val action = WhiskActionMetaData.serdes.read(serializedAction).toExecutableWhiskAction
                val msg = ActivationMessage.serdes.read(serializedMsg)
                val transid = TransactionId.serdes.read(serializedTransid)
                logging.info(this, s"ConfigurableLB: Received ${msg.action.name};${action.get.annotations};${transid.id} from sender: $sender")
                sender ! s"${msg.action.name};${action.get.annotations};${transid.id}"

            case SubscribeAck(Subscribe("refresh", None, `self`)) =>
                logging.info(this, "ConfigurableLB: subscribed to refresh updates.")
        }
    }))

    override val invokerPool =
        invokerPoolFactory.createInvokerPool(
            actorSystem,
            messagingProvider,
            messageProducer,
            sendActivationToInvoker,
            Some(monitor))

    updateConfig("/data/configLB.yml")

    /** Loadbalancer interface methods */
    override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
    override def clusterSize: Int = schedulingState.clusterSize

    /** 1. Publish a message to the loadbalancer */
    override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
      implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

        val isBlackboxInvocation = action.exec.pull
        val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
        val (invokersToUse, stepSizes) =
            if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
            else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)

        val msgParams = msg.content.toJson.convertTo[Map[String, JsValue]]
        logging.info(this, s"Msg params: ${msgParams}")

        //check possible tag mismatch
        val tag: String = getTag(action.annotations).getOrElse("default")
        val invokeTag: String = {
            val tagValue: Option[JsValue] = msgParams.get("tag")
            tagValue match {
                case None => tag
                case Some(v) => v match {
                    case JsString(s) => s
                    case _ => tag
                }
            }
        }

        //check if force refreshing is requested
        val forceRefresh: Boolean = {
            val r: Option[JsValue] = msgParams.get("controller_config_refresh")
            r match {
                case None => false
                case Some(v) => v match {
                    case JsBoolean(s) => s
                    case _ => false
                }
            }
        }

        //get policy index forwarded by nginx
        val policyIndex: Int = {
            val r: Option[JsValue] = msgParams.get("nginx_openwhisk_policy_index")
            r match {
                case None => 0
                case Some(v) => v match {
                    case JsNumber(s) => Math.max(s.toInt, 0)
                    case _ => 0
                }
            }
        }



        if (forceRefresh == true) updateConfig("/data/configLB.yml")
        else logging.info(this, s"ConfigurableLB: No configuration refresh was requested, using old config")

        if (tag != "default" && tag != invokeTag) {
            logging.info(this, s"Tag mismatch in invocation (stored: $tag, provided: $invokeTag)")
            Future.failed(RejectRequest(BadRequest, "Tag mismatch in invocation"))
        }
        else {
            val actionTag = if (tag != invokeTag) invokeTag else tag
            val nodeZoneMap = schedulingState.nodeZoneMap
            val nodeLabelMap = schedulingState.nodeLabelMap

            val configurableLBSettings: ConfigurableLBSettings = schedulingState.configurableLBSettings

            propagateInvocation(action, msg, transid)

            logging.info(this, "choosing invoker...")
            val chosen = if (invokersToUse.nonEmpty) {
                val invoker: Option[(InvokerInstanceId, Boolean)] = ConfigurableLoadBalancer.schedule(
                    action,
                    msg,
                    action.limits.concurrency.maxConcurrent,
                    action.fullyQualifiedName(true),
                    invokersToUse,
                    schedulingState.invokerSlots,
                    action.limits.memory.megabytes,
                    actionTag,
                    stepSizes,
                    nodeZoneMap,
                    nodeLabelMap,
                    configurableLBSettings,
                    controllerInstance,
                    policyIndex)(logging, transid)
                invoker.foreach {
                    case (_, true) =>
                        val metric =
                            if (isBlackboxInvocation)
                                LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
                            else
                                LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
                        MetricEmitter.emitCounterMetric(metric)
                    case _ =>
                }
                invoker.map(_._1)
            } else {
                None
            }

            chosen
              .map { invoker =>
                  // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
                  val memoryLimit = action.limits.memory
                  val memoryLimitInfo = if (memoryLimit == MemoryLimit()) {
                      "std"
                  } else {
                      "non-std"
                  }
                  val timeLimit = action.limits.timeout
                  val timeLimitInfo = if (timeLimit == TimeLimit()) {
                      "std"
                  } else {
                      "non-std"
                  }
                  logging.info(
                      this,
                      s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")
                  val activationResult = setupActivation(msg, action, invoker)
                  sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
              }
              .getOrElse {
                  /* TODO: add extra publish method to be called when an invocation is propagated
                      this way we can handle the invocation promise and forward the result to the previous controller in the "chain"
                      and the actual response to nginx is given by the first contacted controller
                      while the activationResult is collected by the last one, and forwarded up the chain of controllers
                   */
                  /*if chosen == None => policy failed, propagate invocation to next policy
                    check if policies are available
                    if none available, and this is not a propagated request, fail
                    if none are available, and this is a propagated request, propagate failure to original sender (this will be in a different method)
                   */
                  // report the state of all invokers
                  val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
                      val count = agg.getOrElse(curr.status, 0) + 1
                      agg + (curr.status -> count)
                  }

                  logging.error(
                      this,
                      s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
                  Future.failed(LoadBalancerException("No invokers available"))
              }
        }
    }

    override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry) = {
        schedulingState.invokerSlots
          .lift(invoker.toInt)
          .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
    }



    def propagateInvocation(action: ExecutableWhiskActionMetaData, msg: ActivationMessage, transid: TransactionId) = {
        /* TODO: select policy */
        implicit val timeout = Timeout(action.limits.timeout.duration)

        val serializedAction = WhiskActionMetaData.serdes.write(action.toWhiskAction)
        val serializedMsg = ActivationMessage.serdes.write(msg)
        val serializedTransid = TransactionId.serdes.write(transid)

        val future = mediator ? Publish("invocation", PropagateInvocation(serializedAction, serializedMsg, serializedTransid))
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        logging.info(this, s"Result of action propagation: $result")
    }

    def getTag(annotations: Parameters)(implicit logging: Logging, transId: TransactionId): Option[String] = {
        annotations.get("parameters") match {
            case Some(json) =>
                logging.info(this, s"Converting $json into Tag object...")
                val t: List[Map[String, JsValue]] = json.convertTo[List[Map[String, JsValue]]]
                t.find(m => m.contains("tag")) match {
                    case Some(m) =>
                        val tag = m("tag")
                        tag match {
                            case JsString(x) =>
                                logging.info(this, s"Received: $x")
                                Some(x)
                            case _ => None
                        }
                    case None => None
                }
            case None =>
                logging.info(this, "No tag received")
                None
        }
    }

    def loadConfigText(fileName: String): String = {
        val source = Source.fromFile(fileName)
        val text = try source.mkString finally source.close()
        text
    }

    def updateConfig(fileName: String): Unit = {
        val configurationText : String = loadConfigText(fileName)
        val configurableLBSettings : ConfigurableLBSettings = LBControlParser.parseConfigurableLBSettings(configurationText)

        mediator ! Publish("refresh", ConfigurationUpdated(configurableLBSettings))
    }
}

object ConfigurableLoadBalancer extends LoadBalancerProvider {

    override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
      implicit actorSystem: ActorSystem,
      logging: Logging,
      materializer: ActorMaterializer): LoadBalancer = {

        val invokerPoolFactory = new InvokerPoolFactory {
            override def createInvokerPool(
                                            actorRefFactory: ActorRefFactory,
                                            messagingProvider: MessagingProvider,
                                            messagingProducer: MessageProducer,
                                            sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
                                            monitor: Option[ActorRef]): ActorRef = {

                InvokerPool.prepare(instance, WhiskEntityStore.datastore())

                actorRefFactory.actorOf(
                    InvokerPool.props(
                        (f, i) => f.actorOf(InvokerActor.props(i, instance)),
                        (m, i) => sendActivationToInvoker(messagingProducer, m, i),
                        messagingProvider.getConsumer(
                            whiskConfig,
                            s"${Controller.topicPrefix}health${instance.asString}",
                            s"${Controller.topicPrefix}health",
                            maxPeek = 128),
                        monitor))
            }

        }
        new ConfigurableLoadBalancer(
            whiskConfig,
            instance,
            createFeedFactory(whiskConfig, instance),
            invokerPoolFactory)
    }

    def requiredProperties: Map[String, String] = kafkaHosts

    def defaultSchedule(
                         action: ExecutableWhiskActionMetaData,
                         msg: ActivationMessage,
                         invokers: IndexedSeq[InvokerHealth],
                         sameZoneInvokers: IndexedSeq[InvokerHealth],
                         dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
                         stepSizes: Seq[Int],
                         maxC: Option[Int],
                         maxF: Option[Int]
                       )(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {
        val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
        val stepSize = stepSizes(hash % stepSizes.size)

        val invoker = if (sameZoneInvokers.size > 0) {
            val homeInvoker = hash % sameZoneInvokers.size
            ShardingContainerPoolBalancer.schedule(
                maxF.getOrElse(action.limits.concurrency.maxConcurrent),
                action.fullyQualifiedName(true),
                sameZoneInvokers,
                dispatched,
                maxC.getOrElse(action.limits.memory.megabytes),
                homeInvoker,
                stepSize)
        } else None

        val chosenInvoker = invoker match {
            case None =>
                val newHomeInvoker = hash % invokers.size
                ShardingContainerPoolBalancer.schedule(
                    maxF.getOrElse(action.limits.concurrency.maxConcurrent),
                    action.fullyQualifiedName(true),
                    invokers,
                    dispatched,
                    maxC.getOrElse(action.limits.memory.megabytes),
                    newHomeInvoker,
                    stepSize)
            case _ => invoker
        }
        logging.info(this, "Invoker chosen with default scheduler!")
        chosenInvoker
    }


    def filterInvokers(invokerList: IndexedSeq[InvokerHealth], label: Option[String], labelMap: Map[String, String]): IndexedSeq[InvokerHealth] = {
        invokerList.filter(
            i =>
                i.id.uniqueName match {
                    case None => false
                    case Some(n) =>
                        val invokerLabel = labelMap.get(n)
                        invokerLabel.isDefined && invokerLabel == label
                }
        )
    }

    /**
     * Scans through all invokers and searches for an invoker tries to get a free slot on an invoker. If no slot can be
     * obtained, randomly picks a healthy invoker.
     *
     * @param maxConcurrent concurrency limit supported by this action
     * @param invokers a list of available invokers to search in, including their state
     * @param dispatched semaphores for each invoker to give the slots away from
     * @param slots Number of slots, that need to be acquired (e.g. memory in MB)
     * @param index the index to start from (initially should be the "homeInvoker"
     * @param step stable identifier of the entity to be scheduled
     * @return an invoker to schedule to or None of no invoker is available
     */

    def schedule(
                  action: ExecutableWhiskActionMetaData,
                  msg: ActivationMessage,
                  maxConcurrent: Int,
                  fqn: FullyQualifiedEntityName,
                  invokers: IndexedSeq[InvokerHealth],
                  dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
                  slots: Int,
                  tag: String,
                  stepSizes: Seq[Int],
                  nodeZoneMap: Map[String, String],
                  nodeLabelMap: Map[String, String],
                  configurableLBSettings: ConfigurableLBSettings,
                  controllerInstance: ControllerInstanceId,
                  policyIndex: Int
                )(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {

        //TODO: generalize bestFirst/random/byStrategy scheduling to both blocks and workers
        @tailrec
        def bestFirstSchedule(specifiedInvokers: List[InvokerHealth], maxC: Option[Int], maxF: Option[Int]): Option[(InvokerInstanceId, Boolean)] = {
            specifiedInvokers match {
                case IndexedSeq() => None
                case (x: InvokerHealth) :: xs =>
                    if (dispatched(x.id.toInt).tryAcquireConcurrent(
                        fqn, maxF.getOrElse(maxConcurrent), maxC.getOrElse(slots)
                    ))
                        Some(x.id, false)
                    else
                        bestFirstSchedule(xs, maxC, maxF)
            }
        }

        @tailrec
        def randomSchedule(specifiedInvokers:  IndexedSeq[InvokerHealth],
                           maxC : Option[Int], maxF : Option[Int]): Option[(InvokerInstanceId, Boolean)] = {
            def getRandomIndex = {
                val rand = scala.util.Random
                val rIndex = rand.nextInt(specifiedInvokers.length)
                rIndex
            }
            specifiedInvokers match {
                case IndexedSeq() => None
                case (_ : InvokerHealth) +: _ =>
                    val rIndex = getRandomIndex
                    val x = specifiedInvokers(rIndex)
                    logging.info(this, s"ConfigurableLB: Random scheduling chosen... number of invokers: ${specifiedInvokers.length} index chosen: $rIndex")
                    logging.info(this, s"ConfigurableLB: Chosen invoker: ${x}; Chosen invoker InstanceId: ${x.id}; Chosen invoker tags: ${x.id.tags}")
                    if (dispatched(x.id.toInt).tryAcquireConcurrent(
                        fqn, maxF.getOrElse(maxConcurrent), maxC.getOrElse(slots)
                    ))
                        Some(x.id, false)
                    else
                        randomSchedule(specifiedInvokers.take(rIndex - 1) ++ specifiedInvokers.drop(rIndex), maxC, maxF )
            }

        }

        def scheduleByStrategy(specifiedInvokers: IndexedSeq[InvokerHealth],
                               sameZoneInvokers: IndexedSeq[InvokerHealth],
                               strategy: Option[String],
                               maxC: Option[Int],
                               maxF: Option[Int]): Option[(InvokerInstanceId, Boolean)] = {
            strategy match {
                case None => defaultSchedule(action, msg, specifiedInvokers, sameZoneInvokers, dispatched, stepSizes, maxC, maxF)
                case Some("best-first") => bestFirstSchedule(specifiedInvokers.toList, maxC, maxF)
                case Some("random") =>
                    val randomSameZone = randomSchedule(sameZoneInvokers, maxC, maxF)
                    randomSameZone match {
                        case None =>
                            logging.info(this, s"ConfigurableLB: Random scheduling found no invoker in same topological zone. Proceeding with normal random scheduling.")
                            randomSchedule(specifiedInvokers, maxC, maxF)
                        case _ => randomSameZone
                    }
                case Some("platform") => defaultSchedule(action, msg, specifiedInvokers, sameZoneInvokers, dispatched, stepSizes, maxC, maxF)
                case _ => throw new Exception("No strategy found")
            }
        }

        def scheduleOnInvokerLabels(
                                     healthyInvokers: IndexedSeq[InvokerHealth],
                                     labels: List[InvokerLabel],
                                     blockStrategy: Option[String],
                                     blockMaxC: Option[Int],
                                     blockMaxF: Option[Int],
                                   ): Option[(InvokerInstanceId, Boolean)] = {
            labels match {
                case List() => None
                case l :: ls =>
                    val pattern = """(\*)([\w-_]+)?""".r
                    val cInvokers = l.label match {
                        case pattern("*", null) => healthyInvokers
                        case pattern("*", s) =>
                            logging.info(this, s"ConfigurableLB: filtering invokers from ${healthyInvokers} on label ${s}")
                            filterInvokers(healthyInvokers, Some(s), nodeLabelMap)
                        case _ => healthyInvokers
                    }
                    logging.info(this, s"ConfigurableLB: choosing from invoker label ${l.label}; filtered invokers: ${cInvokers}")
                    val strategy = l.strategy.orElse(blockStrategy)
                    val maxCapacity = l.maxCapacity.orElse(blockMaxC)
                    val maxConcurrentInvocations = l.maxConcurrentInvocations.orElse(blockMaxF)
                    val chosenInvoker = scheduleByStrategy(cInvokers, IndexedSeq.empty[InvokerHealth], strategy, maxCapacity, maxConcurrentInvocations)
                    chosenInvoker.orElse(scheduleOnInvokerLabels(healthyInvokers, ls, blockStrategy, blockMaxC, blockMaxF))
            }
        }

        def scheduleOnSpecifiedBlock(
                                      healthyInvokers: IndexedSeq[InvokerHealth],
                                      sameZoneInvokers: IndexedSeq[InvokerHealth],
                                      blockSettings: BlockSettings): Option[(InvokerInstanceId, Boolean)] = {
            logging.info(this, "ConfigurableLB: scheduleOnSpecifiedBlock with settings: " + blockSettings + " and healthy invokers: " + healthyInvokers.map(i => i.id))
            blockSettings.invokersSettings match {
                case InvokerList(names) =>
                    val cInvokers = healthyInvokers.filter(i => names.contains(i.id.uniqueName.getOrElse("")))
                    if (cInvokers.isEmpty) None
                    //empty list for sameZoneInvokers, as we do not prioritize workers in the same topological zone when a list is explicitly specified
                    else scheduleByStrategy(cInvokers, IndexedSeq.empty[InvokerHealth], blockSettings.strategy, blockSettings.maxCapacity, blockSettings.maxConcurrentInvocations)
                case InvokerLabels(labels) =>
                    scheduleOnInvokerLabels(healthyInvokers, labels, blockSettings.strategy, blockSettings.maxCapacity, blockSettings.maxConcurrentInvocations)
                case All() =>
                    scheduleByStrategy(healthyInvokers, sameZoneInvokers, blockSettings.strategy, blockSettings.maxCapacity, blockSettings.maxConcurrentInvocations)
            }
        }

        @tailrec
        def scheduleBasedOnTagSettings(
                                        healthyInvokers: IndexedSeq[InvokerHealth],
                                        sameZoneInvokers: IndexedSeq[InvokerHealth],
                                        blockSettings: BlockSettings,
                                        followUp: Option[String],
                                        defaultFallBack: Option[TagSettings]): Option[(InvokerInstanceId, Boolean)] = {
            logging.info(this, s"ConfigurableLB scheduleBasedOnTagSettings block settings ${blockSettings} and defaultFallback: ${defaultFallBack.isDefined}")
            val res = scheduleOnSpecifiedBlock(healthyInvokers, sameZoneInvokers, blockSettings)
            if (res.isDefined) {
                logging.info(this, s"ConfigurableLB scheduleBasedOnTagSettings invoker found! ${res}")
                res
            } else {
                followUp match {
                    case None | Some("default") =>
                        defaultFallBack match {
                            case None => defaultSchedule(action, msg, invokers, sameZoneInvokers, dispatched, stepSizes, None, None)
                            case Some(ts) =>
                                //TODO: select default policy based on block strategy
                                ts.strategy match {
                                    case None =>
                                        scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(0), ts.followUp, None)
                                    case Some("random") =>
                                        val r = scala.util.Random
                                        val i = r.nextInt(ts.blockSettings.length)
                                        scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(i), ts.followUp, None)
                                    case Some("best-first") =>
                                        scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(0), ts.followUp, None)
                                    case _ =>
                                        scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(0), ts.followUp, None)
                                }
                        }
                    case Some("fail") =>
                        logging.info(this, s"No compatible invokers found and followup is fail. Can't invoke action!")
                        None
                    case Some(s) =>
                        logging.info(this, s"Wrong followup information ${s}, failing.")
                        None
                }
            }
        }

        val tagSettings: Option[TagSettings] = configurableLBSettings.getTagSettings(tag)
        val defaultSettings: Option[TagSettings] = configurableLBSettings.getTagSettings("default")
        val currentZone = nodeZoneMap.get(controllerInstance.controllerNodeName)
        lazy val sameZoneInvokers: IndexedSeq[InvokerHealth] = filterInvokers(invokers, currentZone, nodeZoneMap)

        logging.info(this, s"ConfigurableLB: Scheduling with nodeZoneMap: ${nodeZoneMap}")
        val numInvokers = invokers.size
        if (numInvokers > 0) {
            val healthyInvokers: IndexedSeq[InvokerHealth] = invokers.filter(_.status.isUsable)
            logging.info(this, s"ConfigurableLB: tag = $tag with settings $tagSettings and invokers: $invokers")
            tagSettings match {
                case None =>
                    defaultSettings match {
                        case None =>
                            logging.info(this, "ConfigurableLB: invocation without default and tag, fall back to default invocation.")
                            logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                            defaultSchedule(action, msg, invokers, sameZoneInvokers, dispatched, stepSizes, None, None)
                        case Some(ts) =>
                            scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(policyIndex), ts.followUp, None)
                    }
                case Some(ts) =>
                    logging.info(this, s"ConfigurableLB tagSettings = ${tagSettings}")
                    logging.info(this, s"Chosen policy index = ${policyIndex}")
                    ts.blockSettings(policyIndex).controller match {
                        case AllControllers() =>
                            logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                            scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(policyIndex), ts.followUp, defaultSettings)

                        case ControllerName(name) =>
                            if (name == controllerInstance.controllerNodeName) {
                                logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                                scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(policyIndex), ts.followUp, defaultSettings)
                            } else {
                                ts.blockSettings(0).topology_tolerance match {
                                    case AllTolerance() =>
                                        logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                                        scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(policyIndex), ts.followUp, defaultSettings)
                                    case SameTolerance() =>
                                        val originalZone = nodeZoneMap.get(name)
                                        val originalZoneInvokers = filterInvokers(invokers, originalZone, nodeZoneMap)
                                        logging.info(this, s"ConfigurableLB: invokers in requested topology zone: $originalZoneInvokers")
                                        scheduleBasedOnTagSettings(healthyInvokers, originalZoneInvokers, ts.blockSettings(policyIndex), ts.followUp, defaultSettings)
                                    case NoneTolerance() =>
                                        logging.info(this, s"ConfigurableLB: action invoked in a different controller and topology_tolerance is 'none', failing.")
                                        None
                                }
                            }
                    }
            }
        } else {
            logging.info(this, "Number of invokers is 0")
            None
        }
    }


}


case class ConfigurableLoadBalancerState(
                                          private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                          private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                          private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                          private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
                                          private var _blackboxStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
                                          protected[loadBalancer] var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
                                          IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
                                          private var _clusterSize: Int = 1,
                                          private var _configurableLBSettings: ConfigurableLBSettings = ConfigurableLBSettings(Map.empty[String, TagSettings]),
                                          private var _nodeZoneMap: Map[String, String] = Map.empty[String, String],
                                          private var _nodeLabelMap: Map[String, String] = Map.empty[String, String],
                                          private var _nodePodMap: Map[String, String] = Map.empty[String, String])(
                                          lbConfig: ShardingContainerPoolBalancerConfig =
                                          loadConfigOrThrow[ShardingContainerPoolBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging, controllerInstance: ControllerInstanceId)
{

    // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
    // 1.0 and 2.0.
    // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
    // means, that there is no differentiation between managed and blackbox invokers.
    // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
    // specified in config and adapted to the managed fraction.
    private val managedFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.managedFraction))
    private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, lbConfig.blackboxFraction))
    logging.info(this, s"managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
        TransactionId.loadbalancer)

    /** Getters for the variables, setting from the outside is only allowed through the update methods below */
    def invokers: IndexedSeq[InvokerHealth] = _invokers
    def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
    def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
    def managedStepSizes: Seq[Int] = _managedStepSizes
    def blackboxStepSizes: Seq[Int] = _blackboxStepSizes
    def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots
    def clusterSize: Int = _clusterSize
    def configurableLBSettings: ConfigurableLBSettings = _configurableLBSettings
    //TODO: refresh nodePodMap and nodeZoneMap if the request comes and the information is older than a certain time
    def nodeZoneMap: Map[String, String] = _nodeZoneMap
    def nodeLabelMap: Map[String, String] = _nodeLabelMap
    def nodePodMap: Map[String, String] = _nodePodMap


    def refreshNodeZoneMap(): Map[String, String] = {
        val associations = try {
            scala.io.Source.fromFile("/data/node_zone_map.json").getLines.mkString("\n").parseJson.convertTo[Map[String, String]]
        } catch {
            case ex: FileNotFoundException => Map.empty[String, String]
            case ex: ParsingException => Map.empty[String, String]
        }
        associations
    }

    def refreshNodePodMap(): Map[String, String] = {
        val associations = try {
            scala.io.Source.fromFile("/data/node_pod_map.json").getLines.mkString("\n").parseJson.convertTo[Map[String, String]]
        } catch {
            case ex: FileNotFoundException => Map.empty[String, String]
            case ex: ParsingException => Map.empty[String, String]
        }
        associations
    }

    def refreshNodeLabelMap(): Map[String, String] = {
        val associations = try {
            scala.io.Source.fromFile("/data/node_label_map.json").getLines.mkString("\n").parseJson.convertTo[Map[String, String]]
        } catch {
            case ex: FileNotFoundException => Map.empty[String, String]
            case ex: ParsingException => Map.empty[String, String]
        }
        associations
    }

    def updateConfigurableLBSettings(newConfig: ConfigurableLBSettings): Unit = {
        logging.info(this, s"ConfigurableLB: Updating configuration")
        _configurableLBSettings = newConfig
    }

    /**
     * @param memory
     * @return calculated invoker slot
     */
    private def getInvokerSlot(memory: ByteSize, invokerName: Option[String],
                               controllerZone: String, sameZoneControllersCount: Int,
                               nodeZoneMap: Map[String, String], nodePodMap: Map[String, String],
                               invokerDistribution: String): ByteSize = {
        /*
            Every load balancer has access to entire memory for invokers in its topology zone; if the invoker is in a different zone (or no zone at all), the memory
            can be obtained in different ways depending on the invoker distribution policy:
            - default => all load balancers have same access to invokers; no complete control is given in same topology zone
            - isolated => complete control in same topology zone; no control over different ones; invokers in no zone are split
            - min_memory => complete control in same topology zone; min_memory for different ones; invokers in no zone are split
            - shared => complete control in same topology zone; all other invokers are split; can easily cause overloading

            If multiple controllers are present in the same zone, invokers will be split between them; priority will still be given to them while scheduling.

            Invokers in different topology zones will always have minimum priority while scheduling, unless explicitly specified in configuration.
        */

        logging.info(this, s"Getting invoker slot with distribution: $invokerDistribution")

        val (sameZoneMemory, noZoneMemory, differentZoneMemory) = invokerDistribution match {
            case "isolated" => (memory / sameZoneControllersCount, memory / _clusterSize, 0.MB)
            case "min_memory" => (memory / sameZoneControllersCount, memory / _clusterSize, MemoryLimit.MIN_MEMORY)
            case "shared" => (memory / sameZoneControllersCount, memory / _clusterSize, memory / _clusterSize)
            case "default" => (memory / _clusterSize, memory / _clusterSize, memory / _clusterSize)
        }

        val invokerZone = invokerName match {
            case None => ""
            case Some(name) => nodeZoneMap.getOrElse(name, "")
        }

        val noZone = (invokerZone == "") || (!nodePodMap.keys.toArray.map(node => nodeZoneMap(node)).contains(invokerZone))

        val invokerShardMemorySize = {
            if (noZone) noZoneMemory
            else if (invokerZone != controllerZone) differentZoneMemory
            else sameZoneMemory
        }


        val newTreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY && invokerDistribution != "isolated" && (invokerZone == controllerZone || noZone)) {
            logging.error(
                this,
                s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
                  + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
                  + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
                TransactionId.loadbalancer)
            MemoryLimit.MIN_MEMORY
        } else {
            logging.info(this, s"Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
              + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB} for Invoker ${invokerName}.")(TransactionId.loadbalancer)
            invokerShardMemorySize
        }
        newTreshold
    }

    /**
     * Updates the scheduling state with the new invokers.
     *
     * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
     * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
     *
     * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
     * report the invoker as "Offline".
     *
     * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
     */
    def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
        _nodeZoneMap = refreshNodeZoneMap()
        _nodePodMap = refreshNodePodMap()
        _nodeLabelMap = refreshNodeLabelMap()

        val controllerZone: String = nodeZoneMap.getOrElse(controllerInstance.controllerNodeName, "")
        val distribution: String = controllerInstance.loadBalancerInvokerDistribution
        val sameZoneControllersCount: Int = nodePodMap.keys.count(k => nodeZoneMap.getOrElse(k, "") == controllerZone)

        val oldSize = _invokers.size
        val newSize = newInvokers.size

        // for small N, allow the managed invokers to overlap with blackbox invokers, and
        // further assume that blackbox invokers << managed invokers
        val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
        val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

        _invokers = newInvokers
        _managedInvokers = _invokers.take(managed)
        _blackboxInvokers = _invokers.takeRight(blackboxes)

        val logDetail = if (oldSize != newSize) {
            _managedStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
            _blackboxStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(blackboxes)

            if (oldSize < newSize) {
                // Keeps the existing state..
                val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
                _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
                    new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory, invoker.id.uniqueName, controllerZone, sameZoneControllersCount, nodeZoneMap, nodePodMap, distribution).toMB.toInt)
                }
                val newInvokerDetails = onlyNewInvokers
                  .map(i =>
                      s"${i.id.toString}: ${i.status} / ${getInvokerSlot(i.id.userMemory, i.id.uniqueName, controllerZone, sameZoneControllersCount, nodeZoneMap, nodePodMap, distribution).toMB.MB} of ${i.id.userMemory.toMB.MB}")
                  .mkString(", ")
                s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
            } else {
                s"number of known invokers decreased: new = $newSize, old = $oldSize."
            }
        } else {
            s"no update required - number of known invokers unchanged: $newSize."
        }

        logging.info(
            this,
            s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes. $logDetail")(
            TransactionId.loadbalancer)
        logging.info(this, s"Cluster size ${_clusterSize}, InvokerSlots: ${_invokerSlots}, Invokers: ${_invokers}")(TransactionId.loadbalancer)
    }

    /**
     * Updates the size of a cluster. Throws away all state for simplicity.
     *
     * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
     * scheduler works on outdated invoker-load data which is acceptable.
     *
     * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
     */
    def updateCluster(newSize: Int): Unit = {
        _nodeZoneMap = refreshNodeZoneMap()
        _nodePodMap = refreshNodePodMap()
        _nodeLabelMap = refreshNodeLabelMap()

        val controllerZone: String = nodeZoneMap.getOrElse(controllerInstance.controllerNodeName, "")
        val distribution: String = controllerInstance.loadBalancerInvokerDistribution
        val sameZoneControllersCount: Int = nodePodMap.keys.count(k => nodeZoneMap.getOrElse(k, "") == controllerZone)

        val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
        if (_clusterSize != actualSize) {
            val oldSize = _clusterSize
            _clusterSize = actualSize
            _invokerSlots = _invokers.map { invoker =>
                new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory, invoker.id.uniqueName, controllerZone, sameZoneControllersCount, nodeZoneMap, nodePodMap, distribution).toMB.toInt)
            }
            // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
            val invokerCount = _invokers.size
            val totalInvokerMemory =
                _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory, invoker.id.uniqueName, controllerZone, sameZoneControllersCount, nodeZoneMap, nodePodMap, distribution).toMB).MB
            val averageInvokerMemory =
                if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
                    (totalInvokerMemory / invokerCount).toMB.MB
                } else {
                    0.MB
                }
            logging.info(
                this,
                s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. ${invokerCount} invokers with ${averageInvokerMemory} average memory size - total invoker memory ${totalInvokerMemory}.")(
                TransactionId.loadbalancer)
        }
    }
}

/**
 * Configuration for the cluster created between loadbalancers.
 *
 * @param useClusterBootstrap Whether or not to use a bootstrap mechanism
 */
// case class ClusterConfig(useClusterBootstrap: Boolean)

/**
 * Configuration for the pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 * @param timeoutFactor factor to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 * @param timeoutAddon extra time to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 */
case class ConfigurableLoadBalancerConfig(managedFraction: Double,
                                          blackboxFraction: Double,
                                          timeoutFactor: Int,
                                          timeoutAddon: FiniteDuration)

/**
 * State kept for each activation slot until completion.
 *
 * @param id id of the activation
 * @param namespaceId namespace that invoked the action
 * @param invokerName invoker the action is scheduled to
 * @param memoryLimit memory limit of the invoked action
 * @param timeLimit time limit of the invoked action
 * @param maxConcurrent concurrency limit of the invoked action
 * @param fullyQualifiedEntityName fully qualified name of the invoked action
 * @param timeoutHandler times out completion of this activation, should be canceled on good paths
 * @param isBlackbox true if the invoked action is a blackbox action, otherwise false (managed action)
 * @param isBlocking true if the action is invoked in a blocking fashion, i.e. "somebody" waits for the result
 */
// case class ActivationEntry(id: ActivationId,
//                            namespaceId: UUID,
//                            invokerName: InvokerInstanceId,
//                            memoryLimit: ByteSize,
//                            timeLimit: FiniteDuration,
//                            maxConcurrent: Int,
//                            fullyQualifiedEntityName: FullyQualifiedEntityName,
//                            timeoutHandler: Cancellable,
//                            isBlackbox: Boolean,
//                            isBlocking: Boolean)


/**
 * Custom message to propagate configuration refresh requests
 * @param newConfig contains the new configuration
 */
case class ConfigurationUpdated(newConfig: ConfigurableLBSettings)

/**
 * Custom message to propagate function invocation requests
 * Parameters are the same of the "publish" function
 */
case class PropagateInvocation(action: JsValue, msg: JsValue, transid: JsValue)
