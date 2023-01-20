package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.AdminClient.*
import client.AdminClientSettings
import zio.Unsafe.unsafe
import zio.stream.ZStream
import zio.{Console, ExitCode, Runtime, Scope, ZIO}

object ConsumerGroupsDeleteOffsets {

  def app(groupId: Option[String], topic: Option[String], partitions: Option[Set[Int]]): ZIO[Scope & Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- groupId
      .fold(ZIO.foreach(cfg.groups)(config => client.deleteConsumerGroupOffsets(config.groupId, config.topicName, config.offsets.keySet.map(_.toInt))).tapError(err => ZIO.logError(s"$err")))(group => client.deleteConsumerGroupOffsets(group, topic.getOrElse("LostTopic"), partitions.getOrElse(Set.empty[Int])).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Deleted offsets of consumer group(s) ${groupId.fold(cfg.groups.map(_.groupId).mkString(","))(group => s"$group")} for the given topic partitions if this input combination existed")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("delete-offsets", "delete consumer group offsets for the given topic partitions") {
    val topicOpts = Opts.option[String](long = "topic", short = "t", help = "type the topic name").map(Some(_)).withDefault(None)
    val partitonsOpts = Opts.option[String](long = "partitions", short = "p", help = "type the topic partitions according to the following pattern: partition1,partition2,partition3 etc")
      .map(_.split(",").toSet.map(_.toInt)).map(Some(_)).withDefault(None)
    (topicOpts, partitonsOpts).mapN((topic, part) => (id: Option[String]) => (path: String) => app(id, topic, part).provide(pathLayer(path), Scope.default))
  }

}
