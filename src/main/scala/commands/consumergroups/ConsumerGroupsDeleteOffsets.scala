package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.resultLayer
import config.NestedConfig.nestedConfigLayer
import config.{Config, NestedConfig}
import client.AdminClient.*
import client.AdminClientSettings
import zio.Unsafe.unsafe
import zio.stream.ZStream
import zio.{Console, ExitCode, Runtime, Scope, ZIO}

object ConsumerGroupsDeleteOffsets {

  def app(groupId: Option[String], topic: Option[String], partitions: Option[Set[Int]]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- groupId
        .fold(ZIO.foreach(nested.groups)(config => client.deleteConsumerGroupOffsets(config.groupId, config.topicName, config.offsets.keySet.map(_.toInt))).tapError(err => ZIO.logError(s"$err")))(group => client.deleteConsumerGroupOffsets(group, topic.getOrElse("LostTopic"), partitions.getOrElse(Set.empty[Int])).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Deleted offsets of consumer group(s) ${groupId.fold(nested.groups.map(_.groupId).mkString(","))(group => s"$group")} for the given topic partitions if this input combination existed")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("delete-offsets", "delete consumer group offsets for the given topic partitions") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val idOpts = Opts.option[String](long = "groupId", short = "id", help = "type the consumer group id").map(Some(_)).withDefault(None)
    val topicOpts = Opts.option[String](long = "topicName", short = "topic", help = "type the topic name").map(Some(_)).withDefault(None)
    val partitonsOpts = Opts.option[String](long = "partitions", short = "partitions", help = "type the topic partitions according to the following pattern: partition1,partition2,partition3 etc")
      .map(_.split(",").toSet.map(_.toInt)).map(Some(_)).withDefault(None)
    unsafe { implicit unsafe => (pathOpts, idOpts, topicOpts, partitonsOpts).mapN((path, id, topic, part) => Runtime.default.unsafe.run(app(id, topic, part).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie)) }
  }

}
