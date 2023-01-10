package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.{pathLayer, resultLayer}
import config.NestedConfig.nestedConfigLayer
import config.{Config, NestedConfig}
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}

object ConsumerGroupAlterOffsets {

  def app(groupId: Option[String], topic: Option[String], offsets: Option[Map[Int, Long]]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- groupId
        .fold(ZIO.foreach(nested.inputs)(config => client.alterConsumerGroupOffsets(config.groupId, config.topicName, config.offsets.map(rec => (rec._1.toInt, rec._2.toLong))).tapError(err => ZIO.logError(s"$err"))))(group => client.alterConsumerGroupOffsets(group, topic.getOrElse("LostTopic"), offsets.getOrElse(Map.empty[Int, Long])).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Altered offsets of consumer group(s) ${groupId.fold(nested.inputs.map(_.groupId).mkString(","))(group => s"$group")} unless this(these) consumer group(s) didn't exist")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("alter-offsets", "alter offsets of the given consumer group. Note: applicable only to empty consumer groups") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val idOpts = Opts.option[String](long = "groupId", short = "id", help = "type the consumer group id").map(Some(_)).withDefault(None)
    val topicOpts = Opts.option[String](long = "topicName", short = "topic", help = "type the topic name").map(Some(_)).withDefault(None)
    val offsetsOpts = Opts.option[String](long = "offsetsData", short = "offsets", help = "type the offsets information according to the following pattern: partition1=offsetvalue1,partition2=offsetvalue2")
      .map(_.split(",").flatMap(_.split("=")).toList.grouped(2).map { case List(a, b) => (a.toInt, b.toLong) }.toMap).map(Some(_)).withDefault(None)
    unsafe { implicit unsafe => (pathOpts, idOpts, topicOpts, offsetsOpts).mapN((path, id, topic, offsets) => Runtime.default.unsafe.run(app(id, topic, offsets).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie)) }
  }

}
