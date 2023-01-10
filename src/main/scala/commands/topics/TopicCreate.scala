package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.{pathLayer, resultLayer}
import config.NestedConfig.nestedConfigLayer
import config.{Config, NestedConfig}
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}

object TopicCreate {

  def app(name: Option[String], numPartitions: Option[Int], replicationFactor: Option[Short], configs: Option[Map[String, String]]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- name
      .fold(client.createTopics(nested.topics.map(_.newTopic)).tapError(err => ZIO.logError(s"$err")))(topicName => client.createTopic(NewTopic(topicName, numPartitions, replicationFactor.map(x => java.lang.Short.valueOf(x)), configs.getOrElse(Map.empty[String, String]))).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Created topic(s) ${name.fold(nested.topics.map(_.topicName).mkString(","))(topicName => s"$topicName")} unless it(they) existed already")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("create", "create a topic with given configs") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val nameOpts = Opts.option[String](long = "topicName", short = "name", help = "type the topic name").map(Some(_)).withDefault(None)
    val partitionOpts = Opts.option[Int](long = "numPartitions", short = "part", help = "type the number of topic partitions").map(Some(_)).withDefault(None)
    val replicationFactorOpts = Opts.option[Short](long = "replicationFactor", short = "replica", help = "type the topic replication factor").map(Some(_)).withDefault(None)
    val configsOpts = Opts.option[String](long = "topicConfigs", short = "configs", help = "type the topic configs in the following way: key1=value1,key2=value2 etc")
      .map(_.split(",").flatMap(_.split("=")).toList.grouped(2).map { case List(a, b) => (a, b) }.toMap).map(Some(_)).withDefault(None)
    unsafe {implicit unsafe => (pathOpts, nameOpts, partitionOpts, replicationFactorOpts, configsOpts).mapN((path, name, part, rep, configs) => Runtime.default.unsafe.run(app(name, part, rep, configs).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie))}
  }

}
