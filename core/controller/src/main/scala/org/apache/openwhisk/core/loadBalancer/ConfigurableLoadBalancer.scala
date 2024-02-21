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
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException

import java.util.concurrent.ThreadLocalRandom


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

            case PropagateInvocation(serializedAction, serializedMsg, serializedTransid, policyIndex, actionTag, availableIndexes, selectedController) =>
                val action = WhiskActionMetaData.serdes.read(serializedAction).toExecutableWhiskAction
                val msg = ActivationMessage.serdes.read(serializedMsg)
                implicit val transid = TransactionId.serdes.read(serializedTransid)

                logging.info(this, s"ConfigurableLB: Received ${msg.action.name};${transid.id} from sender: $sender")
                if (selectedController == controllerInstance.controllerNodeName) {
                    logging.info(this, s"Publishing invocation from $sender on $self")
                    sender ! schedulePropagated(action.get, msg, transid, policyIndex, actionTag, availableIndexes)
                }

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
        //logging.info(this, s"Body params: ${msgParams}")

        //check possible tag mismatch
        val tag: String = getTag(action.annotations).getOrElse("default")
        //logging.info(this, s"Action annotations: ${tag}")

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
        //else logging.info(this, s"ConfigurableLB: No configuration refresh was requested, using old config")

        if (tag != "default" && tag != invokeTag) {
            //logging.info(this, s"Tag mismatch in invocation (stored: $tag, provided: $invokeTag)")
            Future.failed(RejectRequest(BadRequest, "Tag mismatch in invocation"))
        }
        else {
            val actionTag = if (tag != invokeTag) invokeTag else tag
            val nodeZoneMap = schedulingState.nodeZoneMap
            val nodeLabelMap = schedulingState.nodeLabelMap

            val configurableLBSettings: ConfigurableLBSettings = schedulingState.configurableLBSettings

            //logging.info(this, "choosing invoker...")
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
                  /*
                    if no invoker is found, we try to propagate the invocation according to the block settings;
                    this can still return None, causing the overall invocation to fail
                   */
                  .orElse({
                      //logging.info(this, "ConfigurableLB: no invoker found, propagating invocation")
                      propagateInvocation(action, msg, transid, policyIndex, actionTag, None)
                  })
                  .orElse({
                      //logging.info(this, "ConfigurableLB: propagation failed, trying followup options")
                      /* if no tag settings are defined for the given tag, followUp is default;
                        if they are defined, but no followUp is given, followUp is default;
                        if they are defined, and followUp is specified, we use the given string
                       */
                      /* this is None only if no tag settings are found */
                      val followUp: Option[String] = configurableLBSettings.getTagSettings(actionTag).map(_.followUp.getOrElse("default"))
                      followUp match {
                          case None | Some("default") =>
                              //logging.info(this, s"ConfigurableLB: Found followUp $followUp, trying with default")
                              /* policyIndex is -1 because no index has been used yet;
                                propagateInvocation will fail if no tagSettings are defined for the "default" tag
                               */
                              propagateInvocation(action, msg, transid, -1, "default", None)
                          case Some("fail") =>
                              //logging.info(this, s"ConfigurableLB: No compatible invokers found and followup is fail. Can't invoke action!")
                              None
                          case Some(s) =>
                              //logging.info(this, s"ConfigurableLB: Wrong followup information ${s}, failing")
                              None
                      }

                  })

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
//                  logging.info(
//                      this,
//                      s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB (${memoryLimitInfo}), time limit ${timeLimit.duration.toMillis} ms (${timeLimitInfo}) to ${invoker}")
                  val activationResult = setupActivation(msg, action, invoker)
                  sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
              }
              .getOrElse {
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



    def propagateInvocation(action: ExecutableWhiskActionMetaData, msg: ActivationMessage, transid: TransactionId,
                            policyIndex: Int, actionTag: String, availableIndexes: Option[List[Int]]): Option[(InvokerInstanceId, Boolean)] = {
        val configurableLBSettings: ConfigurableLBSettings = schedulingState.configurableLBSettings
        val tagSettings = configurableLBSettings.getTagSettings(actionTag)

        /* if no tag settings are found, we simply fail (nothing to propagate) */
        tagSettings.flatMap(ts => {
            implicit val timeout: Timeout = Timeout(action.limits.timeout.duration)

            val serializedAction = WhiskActionMetaData.serdes.write(action.toWhiskAction)
            val serializedMsg = ActivationMessage.serdes.write(msg)
            val serializedTransid = TransactionId.serdes.write(transid)

            val strategy = ts.strategy.getOrElse("best-first")

            val indexes: Option[List[Int]] = availableIndexes match {
                /* if None is given, it means that this is the first call to propagateInvocation, on the first controller */
                case None =>
                    val oldIndexes = List.range(0, ts.blockSettings.length)
                    val idxList = oldIndexes.filter(_ != policyIndex)
                    if (idxList.nonEmpty) Some(idxList)
                    else None
                case Some(List()) =>
                    /* no need to proceed further, the invocation cannot be propagated */
                    None
                case Some(l) =>
                    Some(l)
            }

            indexes.flatMap(idxs => {
                val nextPolicyIndex: Int = strategy match {
                    case "best-first" => idxs(0)
                    case "random" =>
                        val rand = scala.util.Random
                        val rIndex = rand.nextInt(idxs.length)
                        idxs(rIndex)
                    case _ => idxs(0)
                }
                val newIndexes = idxs.filter(_ != nextPolicyIndex)
                val selectedController = ts.blockSettings(nextPolicyIndex).controller

                selectedController match {
                    case AllControllers() =>
                        /* if no controller is required, then we simply schedule on the same node */
                        schedulePropagated(action, msg, transid, nextPolicyIndex, actionTag, newIndexes)
                    case ControllerName(controllerName) =>
                        val future = mediator ? Publish("invocation",
                            PropagateInvocation(
                                serializedAction,
                                serializedMsg,
                                serializedTransid,
                                nextPolicyIndex,
                                actionTag,
                                newIndexes,
                                controllerName
                            )
                        )

                        Await.result(future, timeout.duration).asInstanceOf[Option[(InvokerInstanceId, Boolean)]]

                }
            })
        })

    }

    def schedulePropagated(action: ExecutableWhiskActionMetaData, msg: ActivationMessage, transid: TransactionId,
                           policyIndex: Int, actionTag: String, availableIndexes: List[Int]): Option[(InvokerInstanceId, Boolean)] = {
        val isBlackboxInvocation = action.exec.pull
        val (invokersToUse, stepSizes) =
            if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
            else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)

        val nodeZoneMap = schedulingState.nodeZoneMap
        val nodeLabelMap = schedulingState.nodeLabelMap
        val configurableLBSettings: ConfigurableLBSettings = schedulingState.configurableLBSettings

        val chosenInvoker = ConfigurableLoadBalancer.schedule(
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

        /* if no invoker is found, we propagate further, until propagateInvocation simply return None */
        chosenInvoker.orElse(propagateInvocation(action, msg, transid, policyIndex, actionTag, Some(availableIndexes)))
    }

    def getTag(annotations: Parameters)(implicit logging: Logging, transId: TransactionId): Option[String] = {
        annotations.get("tag") match {
            case Some(tag) =>
              tag match {
                case JsString(x) =>
                  //logging.info(this, s"Received: $x")
                  Some(x)
                case _ => None
              }
            case None =>
                //logging.info(this, "No tag received")
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


    /* Modified schedule function taken from ShardingContainerPoolBalancer, to allow worker-wise invalidation options */
    @tailrec
    def modifiedDefaultSchedule(
                  action: ExecutableWhiskActionMetaData,
                  fqn: FullyQualifiedEntityName,
                  invokers: IndexedSeq[InvokerHealth],
                  dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
                  invalidateMap: Map[String, (Int, Int)],
                  maxCap: Option[Int],
                  maxCon: Option[Int],
                  index: Int,
                  step: Int,
                  stepsDone: Int = 0)(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {
      val numInvokers = invokers.size

      if (numInvokers > 0) {
        val invoker = invokers(index)
        //test this invoker - if this action supports concurrency, use the scheduleConcurrent function
        val invokerInvalidate = invalidateMap.get(invoker.id.uniqueName.getOrElse("")).getOrElse(
          maxCap.getOrElse(action.limits.memory.megabytes),
          maxCon.getOrElse(action.limits.concurrency.maxConcurrent)
        )
        if (invoker.status.isUsable && dispatched(invoker.id.toInt).tryAcquireConcurrent(fqn, invokerInvalidate._2, invokerInvalidate._1)) {
          Some(invoker.id, false)
        } else {
          // If we've gone through all invokers
          if (stepsDone == numInvokers + 1) {
            val healthyInvokers = invokers.filter(_.status.isUsable)
            if (healthyInvokers.nonEmpty) {
              // Choose a healthy invoker randomly
              val random = healthyInvokers(ThreadLocalRandom.current().nextInt(healthyInvokers.size))
              val invokerInvalidate = invalidateMap.get(random.id.uniqueName.getOrElse("")).getOrElse(
                maxCap.getOrElse(action.limits.memory.megabytes),
                maxCon.getOrElse(action.limits.concurrency.maxConcurrent)
              )
              dispatched(random.id.toInt).forceAcquireConcurrent(fqn, invokerInvalidate._2, invokerInvalidate._1)
              logging.warn(this, s"system is overloaded. Chose invoker${random.id.toInt} by random assignment.")
              Some(random.id, true)
            } else {
              None
            }
          } else {
            val newIndex = (index + step) % numInvokers
            modifiedDefaultSchedule(action, fqn, invokers, dispatched, invalidateMap, maxCap, maxCon, newIndex, step, stepsDone + 1)
          }
        }
      } else {
        None
      }
    }

    def defaultSchedule(
                         action: ExecutableWhiskActionMetaData,
                         msg: ActivationMessage,
                         invokers: IndexedSeq[InvokerHealth],
                         sameZoneInvokers: IndexedSeq[InvokerHealth],
                         dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
                         stepSizes: Seq[Int],
                         invalidateMap: Map[String, (Int, Int)],
                         maxCap: Option[Int],
                         maxCon: Option[Int]
                       )(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] = {
        val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
        val stepSize = stepSizes(hash % stepSizes.size)

        val invoker = if (sameZoneInvokers.size > 0) {
            val homeInvoker = hash % sameZoneInvokers.size
            modifiedDefaultSchedule(
                action,
                action.fullyQualifiedName(true),
                sameZoneInvokers,
                dispatched,
                invalidateMap,
                maxCap,
                maxCon,
                homeInvoker,
                stepSize)
        } else None

        val chosenInvoker = if (invokers.size > 0) invoker match {
            case None =>
                val newHomeInvoker = hash % invokers.size
                modifiedDefaultSchedule(
                    action,
                    action.fullyQualifiedName(true),
                    invokers,
                    dispatched,
                    invalidateMap,
                    maxCap,
                    maxCon,
                    newHomeInvoker,
                    stepSize)
            case _ => invoker
        } else None
      
        //logging.info(this, "Invoker chosen with default scheduler!")
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

    /* TODO:
        - outer strategy still the same ~
        - block-level strategy selects:
            - between "wrk:" tags with normal strategy behaviour ~
            - betweek "set:" tags with outer strategy behaviour (unclear how "platform" should work) => selects between different "set:" tags ~
        - "set:" tag strategy works normally => "best-first" works as "platform"
        - invalidate now overrides, both in "wrk:" and "set:" tags ~
        - block-level invalidate is the default value for "set:" and "wrk:" tags ~
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
        def bestFirstSchedule(specifiedInvokers: List[InvokerHealth],
                              invalidateMap: Map[String, (Int, Int)],
                              maxCap: Option[Int],
                              maxCon: Option[Int]): Option[(InvokerInstanceId, Boolean)] = {
            specifiedInvokers match {
                case IndexedSeq() => None
                case (x: InvokerHealth) :: xs =>
                    val invokerInvalidate = invalidateMap.get(x.id.uniqueName.getOrElse("")).getOrElse(
                      maxCap.getOrElse(slots),
                      maxCon.getOrElse(maxConcurrent)
                    )
                    if (dispatched(x.id.toInt).tryAcquireConcurrent(
                        fqn, invokerInvalidate._2, invokerInvalidate._1
                    ))
                        Some(x.id, false)
                    else
                        bestFirstSchedule(xs, invalidateMap, maxCap, maxCon)
            }
        }

        @tailrec
        def randomSchedule(specifiedInvokers:  IndexedSeq[InvokerHealth],
                           invalidateMap: Map[String, (Int, Int)],
                           maxCap: Option[Int],
                           maxCon: Option[Int]): Option[(InvokerInstanceId, Boolean)] = {
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
                    val invokerInvalidate = invalidateMap.get(x.id.uniqueName.getOrElse("")).getOrElse(
                      maxCap.getOrElse(slots),
                      maxCon.getOrElse(maxConcurrent)
                    )
                    //logging.info(this, s"ConfigurableLB: Random scheduling chosen... number of invokers: ${specifiedInvokers.length} index chosen: $rIndex")
                    //logging.info(this, s"ConfigurableLB: Chosen invoker: ${x}; Chosen invoker InstanceId: ${x.id}; Chosen invoker tags: ${x.id.tags}")
                    if (dispatched(x.id.toInt).tryAcquireConcurrent(
                      fqn, invokerInvalidate._2, invokerInvalidate._1
                    ))
                        Some(x.id, false)
                    else
                        randomSchedule(specifiedInvokers.take(rIndex - 1) ++ specifiedInvokers.drop(rIndex), invalidateMap, maxCap, maxCon)
            }

        }

        def scheduleByStrategy(specifiedInvokers: IndexedSeq[InvokerHealth],
                               sameZoneInvokers: IndexedSeq[InvokerHealth],
                               strategy: Option[String],
                               invalidateMap: Map[String, (Int, Int)],
                               maxCap: Option[Int],
                               maxCon: Option[Int]): Option[(InvokerInstanceId, Boolean)] = {
            strategy match {
                case None => defaultSchedule(action, msg, specifiedInvokers, sameZoneInvokers, dispatched, stepSizes, invalidateMap, maxCap, maxCon)
                case Some("best-first") => bestFirstSchedule(specifiedInvokers.toList, invalidateMap, maxCap, maxCon)
                case Some("random") =>
                    val randomSameZone = randomSchedule(sameZoneInvokers, invalidateMap, maxCap, maxCon)
                    randomSameZone match {
                        case None =>
                            //logging.info(this, s"ConfigurableLB: Random scheduling found no invoker in same topological zone. Proceeding with normal random scheduling.")
                            randomSchedule(specifiedInvokers, invalidateMap, maxCap, maxCon)
                        case _ => randomSameZone
                    }
                case Some("platform") => defaultSchedule(action, msg, specifiedInvokers, sameZoneInvokers, dispatched, stepSizes, invalidateMap, maxCap, maxCon)
                case _ => throw new Exception("No strategy found")
            }
        }

        def scheduleOnInvokerLabels(
                                     healthyInvokers: IndexedSeq[InvokerHealth],
                                     sameZoneInvokers: IndexedSeq[InvokerHealth],
                                     sets: List[WorkerSet],
                                     blockStrategy: Option[String],
                                   ): Option[(InvokerInstanceId, Boolean)] = {
            // platform has no meaning in this selection, so it's either random or best-first
            val strategy = blockStrategy match {
              case Some("random") => "random"
              case _ => "best-first"
            }
            sets match {
                case List() => None
                case (l : WorkerSet) :: ls =>
                    //depending on the strategy, we change the actual set being used, as well as the recursive cal
                    val (currentSet, currentIndex) = if (strategy == "best-first") {
                      (l, 0)
                    } else {
                      val rand = scala.util.Random
                      val rIndex = rand.nextInt(sets.length)
                      (sets(rIndex), rIndex)
                    }

                    val cInvokers = currentSet.label match {
                      case WorkerAll() => healthyInvokers
                      case WorkerSubset(s) =>
                        //logging.info(this, s"ConfigurableLB: filtering invokers from ${healthyInvokers} on label ${s} with nodeZoneMap ${nodeZoneMap}")
                        filterInvokers(healthyInvokers, Some(s), nodeZoneMap)
                      case _ => healthyInvokers
                    }
                    //logging.info(this, s"ConfigurableLB: choosing from invoker label ${currentSet.label}; filtered invokers: ${cInvokers}")
                    val maxCap = currentSet.maxCapacity.getOrElse(slots)
                    val maxCon = currentSet.maxConcurrentInvocations.getOrElse(maxConcurrent)
                    // we use the strategy specified in the subset to schedule on it; if not specified, the strategy is "platform"
                    val chosenInvoker = scheduleByStrategy(cInvokers, IndexedSeq.empty[InvokerHealth], currentSet.strategy, Map.empty[String, (Int, Int)], Some(maxCap), Some(maxCon))

                    if (strategy == "best-first") {
                      chosenInvoker.orElse(scheduleOnInvokerLabels(healthyInvokers, sameZoneInvokers, ls, blockStrategy))
                    } else {
                      chosenInvoker.orElse(scheduleOnInvokerLabels(healthyInvokers, sameZoneInvokers, sets.take(currentIndex - 1) ++ sets.drop(currentIndex), blockStrategy))
                    }
            }
        }

        def scheduleOnSpecifiedBlock(
                                      healthyInvokers: IndexedSeq[InvokerHealth],
                                      sameZoneInvokers: IndexedSeq[InvokerHealth],
                                      blockSettings: BlockSettings): Option[(InvokerInstanceId, Boolean)] = {
            //logging.info(this, "ConfiguraebleLB: scheduleOnSpecifiedBlock with settings: " + blockSettings + " and healthy invokers: " + healthyInvokers.map(i => i.id))
            blockSettings.workersSettings match {
                case WorkerList(workers) =>
                  val workerNames = workers.map(_.label)
                  val cInvokers = healthyInvokers.filter(i => workerNames.contains(i.id.uniqueName.getOrElse("")))
                  if (cInvokers.isEmpty) None
                  //empty list for sameZoneInvokers, as we do not prioritize workers in the same topological zone when a list is explicitly specified
                  else {
                    val invalidateMap = workers.map(w => {
                      val maxCap = w.maxCapacity.orElse(blockSettings.maxCapacity).getOrElse(slots)
                      val maxCon = w.maxConcurrentInvocations.orElse(blockSettings.maxConcurrentInvocations).getOrElse(maxConcurrent)
                      w.label -> (maxCap, maxCon)
                    }).toMap
                    /* we use the strategy specified in the block to schedule between single workers
                     * (unlike with WorkerSet, where the strategy selects between them, while the worker selection is done with the WorkerSet strategy)
                     */
                    scheduleByStrategy(cInvokers, IndexedSeq.empty[InvokerHealth], blockSettings.strategy, invalidateMap, None, None)
                  }
                case WorkerSetList(labels) =>
                    scheduleOnInvokerLabels(healthyInvokers, sameZoneInvokers, labels, blockSettings.strategy)
            }
        }

        def scheduleBasedOnTagSettings(healthyInvokers: IndexedSeq[InvokerHealth],
                                        sameZoneInvokers: IndexedSeq[InvokerHealth],
                                        blockSettings: BlockSettings): Option[(InvokerInstanceId, Boolean)] = {
            //logging.info(this, s"ConfigurableLB scheduleBasedOnTagSettings block settings ${blockSettings}")
            val res = scheduleOnSpecifiedBlock(healthyInvokers, sameZoneInvokers, blockSettings)
            //if (res.isDefined)
                //logging.info(this, s"ConfigurableLB scheduleBasedOnTagSettings invoker found! ${res}")

            res
        }

        val tagSettings: Option[TagSettings] = configurableLBSettings.getTagSettings(tag)
        val defaultSettings: Option[TagSettings] = configurableLBSettings.getTagSettings("default")
        val currentZone = nodeZoneMap.get(controllerInstance.controllerNodeName)
        lazy val sameZoneInvokers: IndexedSeq[InvokerHealth] = filterInvokers(invokers, currentZone, nodeZoneMap)

        //logging.info(this, s"ConfigurableLB: Scheduling with nodeZoneMap: ${nodeZoneMap}")
        val numInvokers = invokers.size
        if (numInvokers > 0) {
            val healthyInvokers: IndexedSeq[InvokerHealth] = invokers.filter(_.status.isUsable)
            //logging.info(this, s"ConfigurableLB: tag = $tag with settings $tagSettings and invokers: $invokers")
            tagSettings match {
                case None =>
                    defaultSettings match {
                        case None =>
                            //logging.info(this, "ConfigurableLB: invocation without default and tag, fall back to default invocation.")
                            //logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                            defaultSchedule(action, msg, invokers, sameZoneInvokers, dispatched, stepSizes, Map.empty[String, (Int, Int)], None, None)
                        case Some(ts) =>
                            /* if a default tag is defined, we can simply return None and have the publish() method handle the default invocation (and potential propagation) */
                            None
                    }
                case Some(ts) =>
                    //logging.info(this, s"ConfigurableLB tagSettings = ${tagSettings}")
                    //logging.info(this, s"Chosen policy index = ${policyIndex}")
                    ts.blockSettings(policyIndex).controller match {
                        case AllControllers() =>
                            //logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                            scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(policyIndex))

                        case ControllerName(name) =>
                            if (name == controllerInstance.controllerNodeName) {
                                //logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                                scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(policyIndex))
                            } else {
                                ts.blockSettings(0).topology_tolerance match {
                                    case AllTolerance() =>
                                        //logging.info(this, s"ConfigurableLB: invokers in same topology zone: $sameZoneInvokers")
                                        scheduleBasedOnTagSettings(healthyInvokers, sameZoneInvokers, ts.blockSettings(policyIndex))
                                    case SameTolerance() =>
                                        val originalZone = nodeZoneMap.get(name)
                                        val originalZoneInvokers = filterInvokers(invokers, originalZone, nodeZoneMap)
                                        //logging.info(this, s"ConfigurableLB: invokers in requested topology zone: $originalZoneInvokers")
                                        scheduleBasedOnTagSettings(healthyInvokers, originalZoneInvokers, ts.blockSettings(policyIndex))
                                    case NoneTolerance() =>
                                        //logging.info(this, s"ConfigurableLB: action invoked in a different controller and topology_tolerance is 'none', failing.")
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
 * Custom message to propagate function invocation requests.
 * action, msg and transid parameters are the same of the "publish" function.
 *
 */
case class PropagateInvocation(action: JsValue, msg: JsValue, transid: JsValue,
                               policyIndex: Int, actionTag: String, availableIndexes: List[Int],
                               selectedController: String)
