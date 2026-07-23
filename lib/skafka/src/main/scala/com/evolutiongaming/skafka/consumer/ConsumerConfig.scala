package com.evolutiongaming.skafka.consumer

import com.evolutiongaming.config.ConfigHelper.{FromConf, *}
import com.evolutiongaming.skafka.{CommonConfig, SaslSupportConfig, SslSupportConfig}
import com.typesafe.config.{Config, ConfigException}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig as C

import scala.concurrent.duration.{FiniteDuration, *}

/** Check [[https://kafka.apache.org/documentation/#newconsumerconfigs]]
  */
final case class ConsumerConfig(
  common: CommonConfig                       = CommonConfig.Default,
  groupId: Option[String]                    = None,
  maxPollRecords: Int                        = 500,
  maxPollInterval: FiniteDuration            = 5.minutes,
  sessionTimeout: FiniteDuration             = 10.seconds,
  heartbeatInterval: FiniteDuration          = 3.seconds,
  autoCommit: Boolean                        = true,
  autoCommitInterval: Option[FiniteDuration] = None,
  partitionAssignmentStrategy: String        =
    "org.apache.kafka.clients.consumer.RangeAssignor,org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
  autoOffsetReset: AutoOffsetReset  = AutoOffsetReset.Latest,
  defaultApiTimeout: FiniteDuration = 1.minute,
  fetchMinBytes: Int                = 1,
  fetchMaxBytes: Int                = 52428800,
  fetchMaxWait: FiniteDuration      = 500.millis,
  maxPartitionFetchBytes: Int       = 1048576,
  checkCrcs: Boolean                = true,
  interceptorClasses: List[String]  = Nil,
  excludeInternalTopics: Boolean    = true,
  isolationLevel: IsolationLevel    = IsolationLevel.ReadUncommitted,
  saslSupport: SaslSupportConfig    = SaslSupportConfig.Default,
  sslSupport: SslSupportConfig      = SslSupportConfig.Default,
  clientRack: Option[String]        = None,
  // fork additions (not in upstream skafka 20.2.0) enabling the KIP-848 consumer rebalance protocol
  groupProtocol: Option[GroupProtocol] = None,
  groupRemoteAssignor: Option[String]  = None,
) {

  def bindings: Map[String, String] = {
    val groupIdMap = groupId.fold(Map.empty[String, String]) { groupId => Map((C.GROUP_ID_CONFIG, groupId)) }
    val rackMap = clientRack.fold(Map.empty[String, String]) { a => Map((CommonClientConfigs.CLIENT_RACK_CONFIG, a)) }
    val autoCommitIntervalMap = autoCommitInterval.fold(Map.empty[String, String]) { autoCommitInterval =>
      Map((C.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval.toMillis.toString))
    }
    val bindings = groupIdMap ++ autoCommitIntervalMap ++ Map[String, String](
      (C.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString),
      (C.MAX_POLL_INTERVAL_MS_CONFIG, maxPollInterval.toMillis.toString),
      (C.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout.toMillis.toString),
      (C.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatInterval.toMillis.toString),
      (C.ENABLE_AUTO_COMMIT_CONFIG, autoCommit.toString),
      (C.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy),
      (C.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.toString.toLowerCase),
      (C.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeout.toMillis.toString),
      (C.FETCH_MIN_BYTES_CONFIG, fetchMinBytes.toString),
      (C.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes.toString),
      (C.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait.toMillis.toString),
      (C.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes.toString),
      (C.CHECK_CRCS_CONFIG, checkCrcs.toString),
      (C.INTERCEPTOR_CLASSES_CONFIG, interceptorClasses mkString ","),
      (C.EXCLUDE_INTERNAL_TOPICS_CONFIG, excludeInternalTopics.toString),
      (C.ISOLATION_LEVEL_CONFIG, isolationLevel.name)
    )

    val protocolMap = groupProtocol.fold(Map.empty[String, String]) { p => Map((C.GROUP_PROTOCOL_CONFIG, p.name)) }
    val remoteAssignorMap = groupRemoteAssignor.fold(Map.empty[String, String]) { a =>
      Map((C.GROUP_REMOTE_ASSIGNOR_CONFIG, a))
    }
    // under the KIP-848 consumer protocol these classic-only client properties are unsupported (assignment is
    // server-side; the session/heartbeat timeouts are broker-managed), so they must not be sent. kafka-clients
    // 4.3.0 *throws* `ConfigException` on any of them under `group.protocol=consumer` (its
    // CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS); symmetrically `group.remote.assignor` is consumer-only
    // (CLASSIC_PROTOCOL_UNSUPPORTED_CONFIGS) and throws under classic, so it is emitted only for the consumer
    // protocol.
    val classicOnly = Set(
      C.SESSION_TIMEOUT_MS_CONFIG,
      C.HEARTBEAT_INTERVAL_MS_CONFIG,
      C.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
    )
    val withProtocol =
      if (groupProtocol.contains(GroupProtocol.Consumer)) (bindings -- classicOnly) ++ protocolMap ++ remoteAssignorMap
      else bindings ++ protocolMap

    withProtocol ++ common.bindings ++ rackMap ++ saslSupport.bindings ++ sslSupport.bindings
  }

  def properties: java.util.Properties = {
    val properties = new java.util.Properties
    bindings foreach { case (k, v) => properties.put(k, v) }
    properties
  }
}

object ConsumerConfig {

  val Default: ConsumerConfig = ConsumerConfig()

  private implicit val AutoOffsetResetFromConf: FromConf[AutoOffsetReset] = FromConf[AutoOffsetReset] { (conf, path) =>
    val str   = conf.getString(path)
    val value = AutoOffsetReset.Values.find { _.toString equalsIgnoreCase str }
    value getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse AutoOffsetReset from $str")
    }
  }

  private implicit val IsolationLevelFromConf: FromConf[IsolationLevel] = FromConf[IsolationLevel] { (conf, path) =>
    val str   = conf.getString(path)
    val value = IsolationLevel.Values.find { _.name equalsIgnoreCase str }
    value getOrElse {
      throw new ConfigException.BadValue(conf.origin(), path, s"Cannot parse IsolationLevel from $str")
    }
  }

  def apply(config: Config): ConsumerConfig = {
    apply(config, Default)
  }

  def apply(config: Config, default: => ConsumerConfig): ConsumerConfig = {

    def get[T: FromConf](path: String, paths: String*): Option[T] = {
      config.getOpt[T](path, paths*)
    }

    def getDuration(path: String, pathMs: => String): Option[FiniteDuration] = {
      val value =
        try get[FiniteDuration](path)
        catch { case _: ConfigException => None }
      value orElse get[Long](pathMs).map { _.millis }
    }

    ConsumerConfig(
      common            = CommonConfig(config, default.common),
      groupId           = get[String]("group-id", "group.id") orElse default.groupId,
      maxPollRecords    = get[Int]("max-poll-records", "max.poll.records") getOrElse default.maxPollRecords,
      maxPollInterval   = getDuration("max-poll-interval", "max.poll.interval.ms") getOrElse default.maxPollInterval,
      sessionTimeout    = getDuration("session-timeout", "session.timeout.ms") getOrElse default.sessionTimeout,
      heartbeatInterval =
        getDuration("heartbeat-interval", "heartbeat.interval.ms") getOrElse default.heartbeatInterval,
      autoCommit = get[Boolean]("auto-commit", "enable-auto-commit", "enable.auto.commit") getOrElse default.autoCommit,
      autoCommitInterval =
        getDuration("auto-commit-interval", "auto.commit.interval.ms") orElse default.autoCommitInterval,
      partitionAssignmentStrategy = get[String](
        "partition-assignment-strategy",
        "partition.assignment.strategy"
      ) getOrElse default.partitionAssignmentStrategy,
      autoOffsetReset =
        get[AutoOffsetReset]("auto-offset-reset", "auto.offset.reset") getOrElse default.autoOffsetReset,
      defaultApiTimeout =
        get[FiniteDuration]("default-api-timeout", "default.api.timeout.ms") getOrElse default.defaultApiTimeout,
      fetchMinBytes          = get[Int]("fetch-min-bytes", "fetch.min.bytes") getOrElse default.fetchMinBytes,
      fetchMaxBytes          = get[Int]("fetch-max-bytes", "fetch.max.bytes") getOrElse default.fetchMaxBytes,
      fetchMaxWait           = getDuration("fetch-max-wait", "fetch.max.wait.ms") getOrElse default.fetchMaxWait,
      maxPartitionFetchBytes =
        get[Int]("max-partition-fetch-bytes", "max.partition.fetch.bytes") getOrElse default.maxPartitionFetchBytes,
      checkCrcs          = get[Boolean]("check-crcs", "check.crcs") getOrElse default.checkCrcs,
      interceptorClasses =
        get[List[String]]("interceptor-classes", "interceptor.classes") getOrElse default.interceptorClasses,
      excludeInternalTopics =
        get[Boolean]("exclude-internal-topics", "exclude.internal.topics") getOrElse default.excludeInternalTopics,
      isolationLevel = get[IsolationLevel]("isolation-level", "isolation.level") getOrElse default.isolationLevel,
      saslSupport    = SaslSupportConfig(config, default.saslSupport),
      sslSupport     = SslSupportConfig(config),
      clientRack     = get[String]("client-rack", "client.rack") orElse default.clientRack,
      groupProtocol =
        get[String]("group-protocol", "group.protocol").flatMap(GroupProtocol.parse) orElse default.groupProtocol,
      groupRemoteAssignor =
        get[String]("group-remote-assignor", "group.remote.assignor") orElse default.groupRemoteAssignor,
    )
  }

  // for binary compatibility
  private[consumer] def apply(
    common: CommonConfig,
    groupId: Option[String],
    maxPollRecords: Int,
    maxPollInterval: FiniteDuration,
    sessionTimeout: FiniteDuration,
    heartbeatInterval: FiniteDuration,
    autoCommit: Boolean,
    autoCommitInterval: FiniteDuration,
    partitionAssignmentStrategy: String,
    autoOffsetReset: AutoOffsetReset,
    defaultApiTimeout: FiniteDuration,
    fetchMinBytes: Int,
    fetchMaxBytes: Int,
    fetchMaxWait: FiniteDuration,
    maxPartitionFetchBytes: Int,
    checkCrcs: Boolean,
    interceptorClasses: List[String],
    excludeInternalTopics: Boolean,
    isolationLevel: IsolationLevel,
    saslSupport: SaslSupportConfig,
  ): ConsumerConfig = new ConsumerConfig(
    common                      = common,
    groupId                     = groupId,
    maxPollRecords              = maxPollRecords,
    maxPollInterval             = maxPollInterval,
    sessionTimeout              = sessionTimeout,
    heartbeatInterval           = heartbeatInterval,
    autoCommit                  = autoCommit,
    autoCommitInterval          = Some(autoCommitInterval),
    partitionAssignmentStrategy = partitionAssignmentStrategy,
    autoOffsetReset             = autoOffsetReset,
    defaultApiTimeout           = defaultApiTimeout,
    fetchMinBytes               = fetchMinBytes,
    fetchMaxBytes               = fetchMaxBytes,
    fetchMaxWait                = fetchMaxWait,
    maxPartitionFetchBytes      = maxPartitionFetchBytes,
    checkCrcs                   = checkCrcs,
    interceptorClasses          = interceptorClasses,
    excludeInternalTopics       = excludeInternalTopics,
    isolationLevel              = isolationLevel,
    saslSupport                 = saslSupport,
  )

  // Constructor for backward compatibility (version <= 11.5)
  private[consumer] def apply(
    common: CommonConfig,
    groupId: Option[String],
    maxPollRecords: Int,
    maxPollInterval: FiniteDuration,
    sessionTimeout: FiniteDuration,
    heartbeatInterval: FiniteDuration,
    autoCommit: Boolean,
    autoCommitInterval: FiniteDuration,
    partitionAssignmentStrategy: String,
    autoOffsetReset: AutoOffsetReset,
    defaultApiTimeout: FiniteDuration,
    fetchMinBytes: Int,
    fetchMaxBytes: Int,
    fetchMaxWait: FiniteDuration,
    maxPartitionFetchBytes: Int,
    checkCrcs: Boolean,
    interceptorClasses: List[String],
    excludeInternalTopics: Boolean,
    isolationLevel: IsolationLevel,
  ): ConsumerConfig = new ConsumerConfig(
    common                      = common,
    groupId                     = groupId,
    maxPollRecords              = maxPollRecords,
    maxPollInterval             = maxPollInterval,
    sessionTimeout              = sessionTimeout,
    heartbeatInterval           = heartbeatInterval,
    autoCommit                  = autoCommit,
    autoCommitInterval          = Some(autoCommitInterval),
    partitionAssignmentStrategy = partitionAssignmentStrategy,
    autoOffsetReset             = autoOffsetReset,
    defaultApiTimeout           = defaultApiTimeout,
    fetchMinBytes               = fetchMinBytes,
    fetchMaxBytes               = fetchMaxBytes,
    fetchMaxWait                = fetchMaxWait,
    maxPartitionFetchBytes      = maxPartitionFetchBytes,
    checkCrcs                   = checkCrcs,
    interceptorClasses          = interceptorClasses,
    excludeInternalTopics       = excludeInternalTopics,
    isolationLevel              = isolationLevel,
    saslSupport                 = SaslSupportConfig.Default,
    sslSupport                  = SslSupportConfig.Default,
  )
}
