package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.{pathLayer, resultLayer}
import config.NestedConfig.nestedConfigLayer
import config.{Config, NestedConfig}
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.kafka.consumer.{Consumer, ConsumerSettings}
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO, ZLayer}

import java.time.{LocalDateTime, ZoneOffset}

object ConsumerGroupSetOffsetsToTimestamp {

  def app(groupId: Option[String], topic: Option[String], date: Option[LocalDateTime]): ZIO[Scope & Config & NestedConfig & Consumer, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- groupId
         .fold(ZIO.foreach(nested.inputs)(config => client.setConsumerGroupOffsetsToTimestamp(config.groupId, config.topicName, LocalDateTime.parse(config.date), cfg.bootstrapServers).tapError(err => ZIO.logError(s"$err"))))(group => client.setConsumerGroupOffsetsToTimestamp(group, topic.getOrElse("LostTopic"), date.getOrElse(LocalDateTime.now()), cfg.bootstrapServers).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Altered offsets of consumer group ${groupId.fold(nested.inputs.map(_.groupId).mkString(","))(group => s"$group")} to the given timestamp unless this consumer group didn't exist")
    _ <- client.close
  } yield ExitCode.success

  val layer: ZLayer[Scope & Config, Throwable, Consumer] = ZLayer {
    for {
      cfg <- ZIO.service[Config]
      consumer <- Consumer.make(ConsumerSettings(cfg.bootstrapServers).withGroupId("technical"))
    } yield consumer
  }

  val command: Command[Unit] = Command("set-offsets-to-timestamp", "sets offsets of the given consumer group to given timestamp. Note: applicable only to empty consumer groups") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val idOpts = Opts.option[String](long = "groupId", short = "id", help = "type the consumer group id").map(Some(_)).withDefault(None)
    val topicOpts = Opts.option[String](long = "topicName", short = "topic", help = "type the topic name").map(Some(_)).withDefault(None)
    val dateOpts = Opts.option[String](long = "timestamp", short = "time", help = "type the timestamp according to the following pattern: yyyy-MM-ddTHH:mm:ss.xxx")
      .map(date => LocalDateTime.parse(date)).map(Some(_)).withDefault(None)
    unsafe { implicit unsafe => (pathOpts, idOpts, topicOpts, dateOpts).mapN((path, id, topic, date) => Runtime.default.unsafe.run(app(id, topic, date).provide(resultLayer(path), Scope.default, nestedConfigLayer, layer).orDie)) }
  }

}
