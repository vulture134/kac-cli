package client

import org.apache.kafka.clients.admin.{AlterConsumerGroupOffsetsOptions, DeleteConsumerGroupOffsetsOptions, DescribeTopicsOptions, ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec, ListConsumerGroupsOptions, ListTopicsOptions, AdminClient as JAdminClient, AlterConfigOp as JAlterConfigOp, AlterConfigsOptions as JAlterConfigsOptions, Config as JConfig, ConfigEntry as JConfigEntry, ConsumerGroupListing as JConsumerGroupListing, CreateTopicsOptions as JCreateTopicsOptions, DescribeConfigsOptions as JDescribeConfigsOptions, NewTopic as JNewTopic, TopicDescription as JTopicDescription, TopicListing as JTopicListing}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer as JKafkaConsumer, OffsetAndMetadata as JOffsetAndMetadata}
import org.apache.kafka.common.{KafkaFuture, MetricName, Uuid, ConsumerGroupState as JConsumerGroupState, Node as JNode, PartitionInfo as JPartitionInfo, TopicPartition as JTopicPartition, TopicPartitionInfo as JTopicPartitionInfo}
import org.apache.kafka.common.config.ConfigResource as JConfigResource
import org.apache.kafka.common.{Metric, MetricName}
import zio.*
import zio.kafka.consumer.*

import java.time.{LocalDateTime, OffsetDateTime, ZoneOffset}
import java.lang.Short
import java.util.{Optional, Properties}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

trait AdminClient {
  import AdminClient.*

  def createTopics(newTopics: Iterable[NewTopic], options: Option[CreateTopicsOptions] = None): Task[Unit]

  def createTopic(newTopic: NewTopic, validateOnly: Boolean = false): Task[Unit]

  def createTopicCustom(name: String, configs: Map[String, String], options: Option[CreateTopicsOptions] = None): Task[Unit]

  def deleteTopics(topicsToDelete: Iterable[String]): Task[Unit]

  def deleteTopic(topicToDelete: String): Task[Unit]

  def deleteConsumerGroups(groupsToDelete: Iterable[String]): Task[Unit]

  def deleteConsumerGroup(groupToDelete: String): Task[Unit]

  def deleteConsumerGroupOffsets(groupId: String, topic: String, partitions: Set[Int], options: Option[DeleteConsumerGroupOffsetsOptions] = None): Task[Unit]

  def listTopics(listTopicsOptions: Option[ListTopicsOptions] = None): Task[Map[String, TopicListing]]

  def listConsumerGroups(options: Option[ListConsumerGroupsOptions] = None): Task[Iterable[ConsumerGroupListing]]

  def listConsumerGroupOffsets(groupId: String, options: Option[ListConsumerGroupOffsetsOptions] = None): Task[Map[String, Map[TopicPartition, OffsetAndMetadata]]]

  def listConsumerGroupsOffsets(groupIds: Iterable[String], options: Option[ListConsumerGroupOffsetsOptions] = None): Task[Map[String, Map[TopicPartition, OffsetAndMetadata]]]

  def describeTopics(topicNames: Iterable[String], options: Option[DescribeTopicsOptions] = None): Task[Map[String, TopicDescription]]

  def describeConfigs(configResources: Iterable[ConfigResource], options: Option[DescribeConfigsOptions] = None): Task[Map[ConfigResource, KafkaConfig]]

  def describeTopic(name: String, configsDescOpts: Option[DescribeConfigsOptions] = None, topicDescOpts: Option[DescribeTopicsOptions] = None): Task[AggregatedTopicDescription]

  def describeTopicsCustom(topics: List[String], configsDescOpts: Option[DescribeConfigsOptions] = None, topicDescOpts: Option[DescribeTopicsOptions] = None): Task[List[AggregatedTopicDescription]]

  def describeConsumerGroup(groupId: String): ZIO[Consumer, Throwable, Map[String, Map[TopicPartition, AggregatedOffsetDescription]]]

  def incrementalAlterConfig(configs: Map[ConfigResource, Iterable[AlterConfigOperation]]): Task[Unit]

  def alterTopicConfigs(topic: String, input: Map[String, String]): Task[Unit]

  def alterTopicsConfigs(configsMap: Map[String, Map[String, String]]): Task[Unit]

  def alterConsumerGroupOffsets(groupId: String, topic: String, offsets: Map[Int, Long], options: Option[AlterConsumerGroupOffsetsOptions] = None): Task[Unit]

  def setConsumerGroupOffsetsToTimestamp(groupId: String, topic: String, timestamp: LocalDateTime, bootstrapServers: List[String], options: Option[AlterConsumerGroupOffsetsOptions] = None): ZIO[Consumer, Throwable, Unit]

  def metrics: Task[Map[MetricName, Metric]]

  def close: Task[Unit]

}

object AdminClient {

  private final case class LiveAdminClient(
                                            private[client] val adminClient: JAdminClient
                                          ) extends AdminClient {

    override def createTopics(newTopics: Iterable[NewTopic],
                              options: Option[CreateTopicsOptions] = None): Task[Unit] = {
      val asJava = newTopics.map(_.asJava).asJavaCollection

      fromKafkaFutureVoid {
        ZIO.attemptBlocking(
          options
            .fold(adminClient.createTopics(asJava))(opts => adminClient.createTopics(asJava, opts.asJava))
            .all()
        )
      }
    }

    override def createTopic(newTopic: NewTopic, validateOnly: Boolean): Task[Unit] =
      createTopics(List(newTopic), Some(CreateTopicsOptions(validateOnly)))

    def createTopicCustom(name: String, configs: Map[String, String], options: Option[CreateTopicsOptions] = None): Task[Unit] = for {
      listing <- listTopics()
      _ <- createTopic(NewTopic(name, None, None, configs)).when(!listing.contains(name))
    } yield ()

    override def deleteTopics(topicsToDelete: Iterable[String]): Task[Unit] = {
      val asJava = topicsToDelete.asJavaCollection
      fromKafkaFutureVoid {
        ZIO.attemptBlocking(adminClient.deleteTopics(asJava).all())
      }
    }

    def deleteTopic(topicToDelete: String): Task[Unit] = for {
      listing <- listTopics()
      _ <- deleteTopics(Iterable(topicToDelete)).when(listing.contains(topicToDelete))
    } yield ()

    def deleteConsumerGroups(groupsToDelete: Iterable[String]): Task[Unit] = {
      val asJava = groupsToDelete.asJavaCollection
      fromKafkaFutureVoid {
        ZIO.attemptBlocking(adminClient.deleteConsumerGroups(asJava).all())
      }
    }

    def deleteConsumerGroupOffsets(groupId: String, topic: String, partitions: Set[Int], options: Option[DeleteConsumerGroupOffsetsOptions] = None): Task[Unit] = {
      val asJava = partitions.map(part => new JTopicPartition(topic, part)).asJava

      fromKafkaFutureVoid {
        ZIO.attemptBlocking(
          options.
            fold(adminClient.deleteConsumerGroupOffsets(groupId, asJava))(opts => adminClient.deleteConsumerGroupOffsets(groupId, asJava, opts))
            .all()
        )
      }
    }

    def deleteConsumerGroup(groupToDelete: String): Task[Unit] = for {
      listing <- listConsumerGroups()
      _ <- deleteConsumerGroups(Iterable(groupToDelete)).when(listing.map(_.groupId).iterator.contains(groupToDelete))
    } yield ()

    override def listTopics(listTopicsOptions: Option[ListTopicsOptions] = None): Task[Map[String, TopicListing]] =
      fromKafkaFuture {
        ZIO.attemptBlocking(
          listTopicsOptions.fold(adminClient.listTopics())(opts => adminClient.listTopics(opts)).namesToListings()
        )
      }.map(_.asScala.map{case (k, v) => k -> TopicListing(v)}.toMap)



    override def listConsumerGroups(options: Option[ListConsumerGroupsOptions] = None): Task[Iterable[ConsumerGroupListing]] =
      fromKafkaFuture {
        ZIO.attemptBlocking(
          options.fold(adminClient.listConsumerGroups())(opts => adminClient.listConsumerGroups(opts)).all()
        )
      }.map(_.asScala.map(ConsumerGroupListing.apply))

    override def listConsumerGroupOffsets(groupId: String, options: Option[ListConsumerGroupOffsetsOptions] = None): Task[Map[String, Map[TopicPartition, OffsetAndMetadata]]] =
      fromKafkaFuture {
        ZIO.attemptBlocking(
          options.fold(adminClient.listConsumerGroupOffsets(groupId))(opts => adminClient.listConsumerGroupOffsets(groupId, opts)).all()
        )
      }.map(_.asScala.map(record => (record._1, record._2.asScala.toMap
        .map(nested => (TopicPartition(nested._1.topic(), nested._1.partition()), OffsetAndMetadata(nested._2.offset(), nested._2.metadata()))))).toMap)

    override def listConsumerGroupsOffsets(groupIds: Iterable[String], options: Option[ListConsumerGroupOffsetsOptions] = None): Task[Map[String, Map[TopicPartition, OffsetAndMetadata]]] =
      val asJava = groupIds.map(id => id -> new ListConsumerGroupOffsetsSpec).toMap.asJava

      fromKafkaFuture {
        ZIO.attemptBlocking(
          options.fold(adminClient.listConsumerGroupOffsets(asJava))(opts => adminClient.listConsumerGroupOffsets(asJava, opts)).all()
        )
      }.map(_.asScala.map(record => (record._1, record._2.asScala.toMap
        .map(nested => (TopicPartition(nested._1.topic(), nested._1.partition()), OffsetAndMetadata(nested._2.offset(), nested._2.metadata()))))).toMap)

    override def describeTopics(topicNames: Iterable[String], options: Option[DescribeTopicsOptions] = None): Task[Map[String, TopicDescription]] = {
      val asJava = topicNames.asJavaCollection
      fromKafkaFuture {
        ZIO.attemptBlocking(
          options
            .fold(adminClient.describeTopics(asJava))(opts => adminClient.describeTopics(asJava, opts))
            .allTopicNames()
        )
      }.map(_.asScala.map{case (k, v) => k -> AdminClient.TopicDescription(v)}.toMap)
    }

    override def describeConfigs(configResources: Iterable[ConfigResource], options: Option[DescribeConfigsOptions] = None): Task[Map[ConfigResource, KafkaConfig]] = {
      val asJava = configResources.map(_.asJava).asJavaCollection
      fromKafkaFuture {
        ZIO.attemptBlocking(
          options
            .fold(adminClient.describeConfigs(asJava))(opts => adminClient.describeConfigs(asJava, opts.asJava))
            .all()
        )
      }.map(
        _.asScala.view.map { case (configResource, config) =>
          (ConfigResource(configResource.name()), KafkaConfig(config))
        }.toMap
      )
    }

    override def describeTopic(name: String, configsDescOpts: Option[DescribeConfigsOptions] = None, topicDescOpts: Option[DescribeTopicsOptions] = None): Task[AggregatedTopicDescription] =
      for {
        configs <- describeConfigs(Iterable(ConfigResource(name)), configsDescOpts).map(_.values.head.entries.values)
        description <- describeTopics(Iterable(name), topicDescOpts).map(_.values.head)
      } yield AggregatedTopicDescription(description.name, description.internal, description.partitions, description.authorizedOperations, configs)

    def describeTopicsCustom(topics: List[String], configsDescOpts: Option[DescribeConfigsOptions] = None, topicDescOpts: Option[DescribeTopicsOptions] = None): Task[List[AggregatedTopicDescription]] = for {
      configs <- describeConfigs(topics.map(ConfigResource(_)), configsDescOpts).map(_.map(kv => (kv._1.name, kv._2.entries.values)))
      descriptions <- describeTopics(topics, topicDescOpts)
      combined = (configs.keySet ++ descriptions.keySet).flatMap(k => {
        (configs.get(k), descriptions.get(k)) match {
          case (Some(conf), Some(desc)) => Some(AggregatedTopicDescription(desc.name, desc.internal, desc.partitions, desc.authorizedOperations, conf))
          case _ => None
        }
      }
      ).toList
    } yield combined


    override def incrementalAlterConfig(configs: Map[ConfigResource, Iterable[AlterConfigOperation]]): Task[Unit] = {
      val asJava = configs.map(x => (x._1.asJava, x._2.map(_.asJava).asJavaCollection)).asJava

      fromKafkaFutureVoid {
        ZIO.attemptBlocking(
          adminClient.incrementalAlterConfigs(asJava, new JAlterConfigsOptions())
            .all()
        )
      }
    }

    override def alterTopicConfigs(topic: String, input: Map[String, String]): Task[Unit] = for {
      topic <- ZIO.succeed(ConfigResource(topic))
      configs <- describeConfigs(Iterable(topic))
      store = configs.values.flatMap(cfg => cfg.entries.values.map(entry => entry.name() -> entry.value())).toMap
      diff = (store.keySet ++ input.keySet).flatMap(k => {
        (store.get(k), input.get(k)) match {
          case (Some(store), Some(input)) if store == input => None
          case (Some(_), Some(input)) => Some(AlterConfigOperation.applyWithOpType(k, input, OperationType.Set))
          case (None, Some(input)) => Some(AlterConfigOperation.applyWithOpType(k, input, OperationType.Set))
          case (Some(store), None) => Some(AlterConfigOperation.applyWithOpType(k, store, OperationType.Delete))
          case (None, None) => None
        }
      }
      )
      _ <- incrementalAlterConfig(Map(topic -> diff)).when(diff.nonEmpty)
    } yield ()

    def alterTopicsConfigs(configsMap: Map[String, Map[String, String]]): Task[Unit] = for {
      configs <- describeConfigs(configsMap.keySet.map(ConfigResource(_))).map(_.map(kv => (kv._1.name, kv._2.entries.values)))
      store = configs.view.mapValues(_.map(entry => entry.name() -> entry.value()).toMap).toMap
      diff = (store.keySet ++ configsMap.keySet).flatMap(k => {
        (store.get(k), configsMap.get(k)) match {
          case (Some(store), Some(input)) if store == input => None
          case (Some(store), Some(input)) =>
            val nestedDiff = (store.keySet ++ input.keySet).flatMap(key => {
              (store.get(key), input.get(key)) match {
                case (Some(store), Some(input)) if store == input => None
                case (Some(_), Some(input)) => Some(AlterConfigOperation.applyWithOpType(key, input, OperationType.Set))
                case (None, Some(input)) => Some(AlterConfigOperation.applyWithOpType(key, input, OperationType.Set))
                case (Some(store), None) => Some(AlterConfigOperation.applyWithOpType(key, store, OperationType.Delete))
                case (None, None) => None
              }
            })
            Some(k -> nestedDiff)
          case (None, Some(input)) => Some(k -> input.map(kv => AlterConfigOperation.applyWithOpType(kv._1, kv._2, OperationType.Set)))
          case (Some(store), None) => Some(k -> store.map(kv => AlterConfigOperation.applyWithOpType(kv._1, kv._2, OperationType.Delete)))
          case (None, None) => None
        }
      }
      ).toMap
      _ <- incrementalAlterConfig(diff.map(kv => (ConfigResource(kv._1), kv._2))).when(diff.nonEmpty)
    } yield ()



    def alterConsumerGroupOffsets(groupId: String, topic: String, offsets: Map[Int, Long], options: Option[AlterConsumerGroupOffsetsOptions] = None): Task[Unit] =
      val asJava = offsets.map(rec => (new JTopicPartition(topic, rec._1), new JOffsetAndMetadata(rec._2))).asJava

      fromKafkaFutureVoid {
        ZIO.attemptBlocking(
          options.fold(adminClient.alterConsumerGroupOffsets(groupId, asJava))(opts => adminClient.alterConsumerGroupOffsets(groupId, asJava, opts)).all()
        )
      }

    override def setConsumerGroupOffsetsToTimestamp(groupId: String, topic: String, timestamp: LocalDateTime, bootstrapServers: List[String], options: Option[AlterConsumerGroupOffsetsOptions] = None): ZIO[Consumer, Throwable, Unit] = for {
      consumer <- ZIO.service[Consumer]
      partitions <- describeTopic(topic).map(_.partitions.map(_.partition))
      time = timestamp.atOffset(ZoneOffset.UTC).toInstant.toEpochMilli
      asJava = partitions.map(part => new JTopicPartition(topic, part) -> time).toMap
      getOffset <- ZIO.blocking(consumer.offsetsForTimes(asJava))
      _ <- Console.printLine(s"Offsets for specified timestamp are:\n${getOffset.mkString("\n")}")
      offset = getOffset.view.mapValues(data => new JOffsetAndMetadata(data.offset(), "")).toMap.asJava
      _ <- fromKafkaFutureVoid {
        ZIO.attemptBlocking(
          options.fold(adminClient.alterConsumerGroupOffsets(groupId, offset))(opts => adminClient.alterConsumerGroupOffsets(groupId, offset, opts)).all()
        )
      }
    } yield ()

    def describeConsumerGroup(groupId: String): ZIO[Consumer, Throwable, Map[String, Map[TopicPartition, AggregatedOffsetDescription]]] = for {
      consumer <- ZIO.service[Consumer]
      currentOffsets <- listConsumerGroupOffsets(groupId)
      currentOffsetsAsJava = currentOffsets.view.mapValues(_.map(rec => (rec._1.asJava, rec._2))).toMap
      makeInput = currentOffsets.values.toSet.flatMap(_.keySet.map(_.asJava))
      endOffsets <- consumer.endOffsets(makeInput)
      toMap = Map(groupId -> endOffsets)
      combined = (currentOffsetsAsJava.keySet ++ toMap.keySet).flatMap(k => {
        (currentOffsetsAsJava.get(k), toMap.get(k)) match {
          case (Some(current), Some(end)) =>
            val nestedOffset = (current.keySet ++ end.keySet).flatMap(key => {
              (current.get(key), end.get(key)) match {
                case (Some(current), Some(end)) => Some(TopicPartition(key) -> AggregatedOffsetDescription(current.offset, end, end-current.offset, current.metadata))
                case _ => None
              }
            })
            Some(k -> nestedOffset.toMap)
        }
      }
      ).toMap
    } yield combined

    override def metrics: Task[Map[MetricName, Metric]] = ZIO.attemptBlocking(adminClient.metrics().asScala.toMap)

    override def close: Task[Unit] = ZIO.attemptBlocking(adminClient.close())

  }

  case class NewTopic(name: String,
                      numPartitions: Option[Int] = None,
                      replicationFactor: Option[Short] = None,
                      configs: Map[String, String] = Map()) {
    def asJava: JNewTopic = {
      val jn = new JNewTopic(name, Optional.ofNullable[Integer](numPartitions.map(Integer.valueOf).orNull), Optional.ofNullable[Short](replicationFactor.map(Short.valueOf(_)).orNull))

      if (configs.nonEmpty)
        jn.configs(configs.asJava)

      jn
    }
  }

  def fromKafkaFuture[R, T](kfv: RIO[R, KafkaFuture[T]]): RIO[R, T] =
    kfv.flatMap(f => ZIO.fromCompletionStage(f.toCompletionStage))

  def fromKafkaFutureVoid[R](kfv: RIO[R, KafkaFuture[Void]]): RIO[R, Unit] =
    fromKafkaFuture(kfv).unit


  case class TopicListing(name: String, isInternal: Boolean) {
    val asJava = new JTopicListing(name, Uuid.randomUuid(), isInternal)
  }

  object TopicListing {
    def apply(jtl: JTopicListing): TopicListing = TopicListing(jtl.name(), jtl.isInternal)
  }

  case class ConsumerGroupListing(groupId: String, isSimple: Boolean, state: Option[String]) {
    val asJava = new JConsumerGroupListing(groupId, isSimple, Optional.ofNullable[JConsumerGroupState](state.map(JConsumerGroupState.parse).orNull))
    val mkString = s"groupId: $groupId, isSimple: $isSimple, state: $state"
  }

  object ConsumerGroupListing {
    def apply(jcgl: JConsumerGroupListing): ConsumerGroupListing = ConsumerGroupListing(jcgl.groupId(), jcgl.isSimpleConsumerGroup, jcgl.state.map(ConsumerGroupState(_)).toScala)

  }

  trait ConsumerGroupState {
    def asString: String
  }
  object ConsumerGroupState {

    object Unknown extends ConsumerGroupState {
      lazy val asString: String = JConsumerGroupState.UNKNOWN.toString
    }

    object PreparingRebalance extends ConsumerGroupState {
      lazy val asString: String = JConsumerGroupState.PREPARING_REBALANCE.toString
    }

    object CompletingRebalance extends ConsumerGroupState {
      lazy val asString: String = JConsumerGroupState.COMPLETING_REBALANCE.toString
    }

    object Stable extends ConsumerGroupState {
      lazy val asString: String = JConsumerGroupState.STABLE.toString
    }

    object Dead extends ConsumerGroupState {
      lazy val asString: String = JConsumerGroupState.DEAD.toString
    }

    object Empty extends ConsumerGroupState {
      lazy val asString: String = JConsumerGroupState.EMPTY.toString
    }

    def apply(jcgs: JConsumerGroupState): String = jcgs match {
      case JConsumerGroupState.UNKNOWN => Unknown.asString
      case JConsumerGroupState.PREPARING_REBALANCE => PreparingRebalance.asString
      case JConsumerGroupState.COMPLETING_REBALANCE => CompletingRebalance.asString
      case JConsumerGroupState.STABLE => Stable.asString
      case JConsumerGroupState.DEAD => Dead.asString
      case JConsumerGroupState.EMPTY => Empty.asString
    }

  }

  case class OffsetAndMetadata(offset: Long, metadata: String) {
    val asJava: JOffsetAndMetadata = new JOffsetAndMetadata(offset, metadata)
  }

  object OffsetAndMetadata {
    def apply(jom: JOffsetAndMetadata): OffsetAndMetadata = OffsetAndMetadata(jom.offset(), jom.metadata())

  }

  case class TopicPartition(topic: String, partition: Int) {
    val asJava: JTopicPartition = new JTopicPartition(topic, partition)
  }

  object TopicPartition {
    def apply(jpi: JTopicPartition): TopicPartition = TopicPartition(jpi.topic(), jpi.partition())

  }

  case class TopicDescription(name: String,
                              internal: Boolean,
                              partitions: List[TopicPartitionInfo],
                              authorizedOperations: Option[Set[AclOperation]])

  object TopicDescription {
    def apply(jt: JTopicDescription): TopicDescription = {
      val authorizedOperations = Option(jt.authorizedOperations).map(_.asScala.toSet)
      TopicDescription(
        jt.name,
        jt.isInternal,
        jt.partitions.asScala.toList.map(TopicPartitionInfo.apply),
        authorizedOperations.map(_.map(AclOperation.apply))
      )
    }
  }

  case class AggregatedTopicDescription(name: String,
                                        internal: Boolean,
                                        partitions: List[TopicPartitionInfo],
                                        authorizedOperations: Option[Set[AclOperation]],
                                        configs: Iterable[JConfigEntry]) {
    val mkString = s"Topic name: $name\nNumber of partitions: ${partitions.size}\nTopic configs:\n${configs.toList.sortBy(_.name()).map(rec => s"  ${rec.name()}=${rec.value()}").mkString("\n")}\nInternal status: $internal\nAuthorized operations info: ${authorizedOperations.map(_.mkString("\n"))}"
  }



  case class TopicPartitionInfo(partition: Int, leader: Node, replicas: List[Node], isr: List[Node]) {
    lazy val asJava =
      new JTopicPartitionInfo(partition, leader.asJava, replicas.map(_.asJava).asJava, isr.map(_.asJava).asJava)
  }

  object TopicPartitionInfo {
    def apply(jtpi: JTopicPartitionInfo): TopicPartitionInfo =
      TopicPartitionInfo(
        jtpi.partition(),
        Node(jtpi.leader()),
        jtpi.replicas().asScala.map(Node.apply).toList,
        jtpi.isr().asScala.map(Node.apply).toList
      )
  }

  case class Node(id: Int, host: String, port: Int, rack: Option[String] = None) {
    lazy val asJava: JNode = rack.fold(new JNode(id, host, port))(rack => new JNode(id, host, port, rack))
  }

  object Node {
    def apply(jNode: JNode): Node = Node(jNode.id(), jNode.host(), jNode.port, Option(jNode.rack()))
  }

  case class DescribeConfigsOptions(includeSynonyms: Boolean, includeDocumentation: Boolean) {
    lazy val asJava: JDescribeConfigsOptions =
      new JDescribeConfigsOptions().includeSynonyms(includeSynonyms).includeDocumentation(includeDocumentation)
  }

  case class KafkaConfig(entries: Map[String, JConfigEntry])

  object KafkaConfig {
    def apply(jConfig: JConfig): KafkaConfig =
      KafkaConfig(jConfig.entries().asScala.map(e => e.name() -> e).toMap)
  }

  case class ConfigResource(`type`: ConfigResourceType, name: String) {
    lazy val asJava = new JConfigResource(`type`.asJava, name)
  }

  object ConfigResource {
    def apply(name: String): ConfigResource = ConfigResource(ConfigResourceType.Topic, name)
  }

  trait ConfigResourceType {
    def asJava: JConfigResource.Type
  }

  object ConfigResourceType {

    case object Topic extends ConfigResourceType {
      lazy val asJava = JConfigResource.Type.TOPIC
    }

    case object Broker extends ConfigResourceType {
      lazy val asJava = JConfigResource.Type.BROKER
    }

    case object BrokerLogger extends ConfigResourceType {
      lazy val asJava = JConfigResource.Type.BROKER_LOGGER
    }

    case object Unknown extends ConfigResourceType {
      lazy val asJava = JConfigResource.Type.UNKNOWN
    }

    def apply(jcrt: JConfigResource.Type): ConfigResourceType = jcrt match {
      case JConfigResource.Type.TOPIC         => Topic
      case JConfigResource.Type.BROKER        => Broker
      case JConfigResource.Type.BROKER_LOGGER => BrokerLogger
      case JConfigResource.Type.UNKNOWN => Unknown
    }
  }

  case class AlterConfigOperation(name: String, value: String, operationType: OperationType) {
    lazy val asJava = new JAlterConfigOp(new JConfigEntry(name, value), operationType.asJava)
  }

  object AlterConfigOperation {
    def apply(name: String, value: String): AlterConfigOperation = AlterConfigOperation(name, value, OperationType.Set)
    def applyWithOpType(name: String, value: String, `type`: OperationType): AlterConfigOperation = AlterConfigOperation(name, value, `type`)
  }

  trait OperationType {
    def asJava: JAlterConfigOp.OpType
  }

  object OperationType {

    case object Set extends OperationType {
      lazy val asJava = JAlterConfigOp.OpType.SET
    }

    case object Append extends OperationType {
      lazy val asJava = JAlterConfigOp.OpType.APPEND
    }

    case object Delete extends OperationType {
      lazy val asJava = JAlterConfigOp.OpType.DELETE
    }

    case object Subtract extends OperationType {
      lazy val asJava = JAlterConfigOp.OpType.SUBTRACT
    }

    def apply(jot: JAlterConfigOp.OpType): OperationType = jot match {
      case JAlterConfigOp.OpType.SET => Set
      case JAlterConfigOp.OpType.APPEND => Append
      case JAlterConfigOp.OpType.DELETE => Delete
      case JAlterConfigOp.OpType.SUBTRACT => Subtract
    }
  }

  case class AggregatedOffsetDescription(currentOffset: Long, endOffset: Long, lag: Long, metadata: String)

  case class CreateTopicsOptions(validateOnly: Boolean) {
    lazy val asJava: JCreateTopicsOptions = new JCreateTopicsOptions().validateOnly(validateOnly)
  }

  def javaClientFromSettings(settings: AdminClientSettings): ZIO[Scope, Throwable, JAdminClient] =
    ZIO.acquireRelease(ZIO.attempt(JAdminClient.create(settings.driverSettings.asJava)))(client =>
      ZIO.succeed(client.close(settings.closeTimeout))
    )

  def make(settings: AdminClientSettings): ZIO[Scope, Throwable, AdminClient] =
    fromManagedJavaClient(javaClientFromSettings(settings))

  def fromJavaClient(javaClient: JAdminClient): UIO[AdminClient] =
    ZIO.succeed(LiveAdminClient(javaClient))

  def fromManagedJavaClient[R, E](managedJavaClient: ZIO[R with Scope, E, JAdminClient]): ZIO[R with Scope, E, AdminClient] =
    managedJavaClient.flatMap {javaClient =>
      fromJavaClient(javaClient)
    }

  def metrics = ZIO.serviceWithZIO[AdminClient](_.metrics)

}