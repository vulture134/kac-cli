package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.kafka.consumer.{Consumer, ConsumerSettings}
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO, ZLayer}

import java.time.{LocalDateTime, ZoneOffset}

object ConsumerGroupSetOffsetsToTimestamp {

  def app(groupId: Option[String], topic: Option[String], date: Option[LocalDateTime]): ZIO[Scope & Config & Consumer, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- groupId
      .fold(ZIO.foreach(cfg.groups)(config => client.setConsumerGroupOffsetsToTimestamp(config.groupId, config.topicName, LocalDateTime.parse(config.date), cfg.bootstrapServers).tapError(err => ZIO.logError(s"$err"))))(group => client.setConsumerGroupOffsetsToTimestamp(group, topic.getOrElse("LostTopic"), date.getOrElse(LocalDateTime.now()), cfg.bootstrapServers).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Altered offsets of consumer group ${groupId.fold(cfg.groups.map(_.groupId).mkString(","))(group => s"$group")} to the given timestamp unless this consumer group didn't exist")
    _ <- client.close
  } yield ExitCode.success

  val layer: ZLayer[Scope & Config, Throwable, Consumer] = ZLayer {
    for {
      cfg <- ZIO.service[Config]
      consumer <- Consumer.make(ConsumerSettings(cfg.bootstrapServers).withGroupId("technical"))
    } yield consumer
  }

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("set-offsets-to-timestamp", "sets offsets of the given consumer group to given timestamp. Note: applicable only to empty consumer groups") {
    val topicOpts = Opts.option[String](long = "topic", short = "t", help = "type the topic name").map(Some(_)).withDefault(None)
    val dateOpts = Opts.option[String](long = "timestamp", short = "d", help = "type the timestamp according to the following pattern: yyyy-MM-ddTHH:mm:ss.xxx")
      .map(date => LocalDateTime.parse(date)).map(Some(_)).withDefault(None)
    (topicOpts, dateOpts).mapN((topic, date) => (id: Option[String]) => (path: String) => app(id, topic, date).provide(pathLayer(path), Scope.default, layer))
  }

}
