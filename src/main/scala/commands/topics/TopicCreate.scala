package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}

object TopicCreate {

  def app(name: Option[String], numPartitions: Option[Int], replicationFactor: Option[Short], configs: Option[Map[String, String]]): ZIO[Scope & Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- name
      .fold(client.createTopics(cfg.topics.map(_.newTopic)).tapError(err => ZIO.logError(s"$err")))(topicName => client.createTopic(NewTopic(topicName, numPartitions, replicationFactor.map(x => java.lang.Short.valueOf(x)), configs.getOrElse(Map.empty[String, String]))).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Created topic(s) ${name.fold(cfg.topics.map(_.topicName).mkString(","))(topicName => s"$topicName")} unless it(they) existed already")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("create", "create a topic with given configs") {
    val partitionOpts = Opts.option[Int](long = "numPartitions", short = "n", help = "type the number of topic partitions").map(Some(_)).withDefault(None)
    val replicationFactorOpts = Opts.option[Short](long = "replicationFactor", short = "f", help = "type the topic replication factor").map(Some(_)).withDefault(None)
    val configsOpts = Opts.option[String](long = "topicConfigs", short = "p", help = "type the topic configs in the following way: key1=value1,key2=value2 etc")
      .map(_.split(",").flatMap(_.split("=")).toList.grouped(2).map { case List(a, b) => (a, b) }.toMap).map(Some(_)).withDefault(None)
    (partitionOpts, replicationFactorOpts, configsOpts).mapN((part, rep, configs) => (name: Option[String]) => (path: String) => app(name, part, rep, configs).provide(pathLayer(path), Scope.default))
  }

}
