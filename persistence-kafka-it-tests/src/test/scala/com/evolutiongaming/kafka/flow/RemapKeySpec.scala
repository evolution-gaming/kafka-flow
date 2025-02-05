package com.evolutiongaming.kafka.flow

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.KafkaModule
import com.evolutiongaming.kafka.flow.kafkapersistence.{KafkaPersistenceModuleOf, kafkaEagerRecovery}
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, ConsumerRecord}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf, ProducerRecord, RecordMetadata}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import com.evolutiongaming.skafka.Partition
import scala.util.Random

class RemapKeySpec extends ForAllKafkaSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()

  private def producerConfig =
    ProducerConfig(common = CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers)))

  private val appId       = "app-id"
  private val testGroupId = "group-id"

  private val stateTopic = "state-topic-remap-key"

  private def kafkaPersistenceModuleOf: Resource[IO, KafkaPersistenceModuleOf[IO, String]] = {
    ProducerOf
      .apply1[IO]()
      .apply(producerConfig)
      .map { producer =>
        KafkaPersistenceModuleOf.caching[IO, String](
          consumerOf = ConsumerOf.apply1[IO](),
          producer   = producer,
          consumerConfig = ConsumerConfig(
            common          = producerConfig.common,
            autoCommit      = false,
            autoOffsetReset = AutoOffsetReset.Earliest,
            groupId         = testGroupId.some,
          ),
          snapshotTopic = stateTopic
        )
      }
      .evalTap(_ => createTopic(stateTopic, 4))
  }

  test("remap keys but store snapshots in the same partition as input records") {
    // using unique input topic name per test as weaver is running tests in parallel
    val inputTopic = "kafka-persistence-test-remap-key-input"
    kafkaPersistenceModuleOf
      .use { persistenceModuleOf =>
        val kafka    = kafkaModule()
        val remapKey = RemapKey.of((key, _) => IO.pure(key + "-remapped"))
        def findRecord(key: String, records: List[ConsumerRecord[String, String]]) =
          records
            .find(_.key.get.value == key)
            .fold(IO.raiseError[ConsumerRecord[String, String]](new RuntimeException(s"$key not found")))(IO.pure)

        for {
          _ <- createTopic(inputTopic, 4)
          _ <- runFlow(kafka, persistenceModuleOf, inputTopic, remapKey).use { _ =>
            for {
              // send input records to specific partitions deliberately
              input1 <- sendInput(kafka, inputTopic, key = "key0", value = "1", partition = 1)
              input2 <- sendInput(kafka, inputTopic, key = "key1", value = "2", partition = 3)

              // consume snapshots created by the flow
              snapshots <- consume(2, stateTopic)

              // find snapshots by remapped keys and check that they are in the same partition as input records
              // despite the keys were remapped
              snapshot1 <- findRecord("key0-remapped", snapshots)
              snapshot2 <- findRecord("key1-remapped", snapshots)

              _ = assertEquals(snapshot1.partition, input1.partition)
              _ = assertEquals(snapshot2.partition, input2.partition)
            } yield ()
          }
        } yield ()
      }
      .unsafeRunSync()
  }

  private def createTopic(topic: String, partitions: Int) = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)

    Resource.make(IO.delay(AdminClient.create(props)))(cl => IO(cl.close())).use { client =>
      IO(client.createTopics(List(new NewTopic(topic, partitions, 1.toShort)).asJava)).map(res =>
        res.all().get(10, TimeUnit.SECONDS)
      )
    }
  }

  private def consume(n: Int, inputTopic: String): IO[List[ConsumerRecord[String, String]]] = {
    val config =
      ConsumerConfig(
        common          = producerConfig.common,
        autoCommit      = false,
        autoOffsetReset = AutoOffsetReset.Earliest,
        groupId         = Random.alphanumeric.take(10).mkString.some
      )
    ConsumerOf.apply1[IO]().apply[String, String](config).use { consumer =>
      consumer.subscribe(NonEmptySet.one(inputTopic)).flatMap { _ =>
        def poll(acc: List[ConsumerRecord[String, String]]): IO[List[ConsumerRecord[String, String]]] =
          consumer.poll(1.second).flatMap { consumerRecords =>
            val newRecords = acc ++ consumerRecords.values.values.map(_.toList).flatten.toList
            if (newRecords.size < n) {
              poll(newRecords)
            } else IO.pure(newRecords)
          }

        poll(List.empty).timeout(30.seconds)
      }
    }
  }

  private def sendInput(
    kafka: KafkaModule[IO],
    inputTopic: String,
    partition: Int,
    key: String,
    value: String
  ): IO[RecordMetadata] = {
    val config = producerConfig.copy(common = producerConfig.common.copy(clientId = Some("RemapKeySpec-producer")))
    kafka.producerOf(config).use { producer =>
      val record = ProducerRecord[String, String](inputTopic, value.some, key.some, Partition.unsafe(partition).some)
      producer.send(record).flatten
    }
  }

  private def runFlow(
    kafka: KafkaModule[IO],
    persistenceModuleOf: KafkaPersistenceModuleOf[IO, String],
    inputTopic: String,
    remapKey: RemapKey[IO],
  ): Resource[IO, IO[Unit]] = {
    implicit val retry = Retry.empty[IO]
    for {
      flowOf <- topicFlowOf(persistenceModuleOf, remapKey).toResource
      completion <- KafkaFlow
        .resource(
          consumer = kafka.consumerOf("groupId-RemapKeySpec"),
          flowOf = ConsumerFlowOf[IO](
            topic  = inputTopic,
            flowOf = flowOf
          )
        )
    } yield completion
  }

  private def topicFlowOf(
    persistenceModuleOf: KafkaPersistenceModuleOf[IO, String],
    remapKey: RemapKey[IO],
  ): IO[TopicFlowOf[IO]] = {
    for {
      timersOf <- TimersOf.memory[IO, KafkaKey]
      partitionFlowOf = kafkaEagerRecovery[IO, String](
        kafkaPersistenceModuleOf = persistenceModuleOf,
        applicationId            = appId,
        groupId                  = testGroupId,
        timersOf                 = timersOf,
        timerFlowOf = TimerFlowOf
          .persistPeriodically[IO](
            // 0 seconds intervals are used to persist state after every consumer.poll
            // to simplify test scenarios
            fireEvery    = 0.seconds,
            persistEvery = 0.seconds,
            // flush on revoke is set to false, as it has no impact on test outcomes
            // coz we persist the state after every consumer.poll
            flushOnRevoke = false
          ),
        fold = foldLogic,
        partitionFlowConfig = PartitionFlowConfig(
          // 0 seconds intervals are used to commit offsets after every consumer.poll
          // to simplify test scenarios
          triggerTimersInterval = 0.seconds,
          commitOffsetsInterval = 0.seconds
        ),
        tick     = TickOption.id[IO, String],
        filter   = none,
        remapKey = remapKey.some,
        registry = EntityRegistry.empty[IO, KafkaKey, String]
      )
    } yield TopicFlowOf(partitionFlowOf)
  }

  private def foldLogic: FoldOption[IO, String, ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      for {
        input   <- IO(record.value.get.value.decodeUtf8.toOption.get)
        _        = record.key.get.value
        newState = state.fold(input)(_ + input)
      } yield newState.some
    }
}
