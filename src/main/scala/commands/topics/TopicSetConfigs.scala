package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}

object TopicSetConfigs {

  def app(name: Option[String], configs: Option[Map[String, String]]): ZIO[Scope & Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    conf = cfg.topics.map(cfg => (cfg.topicName, cfg.topicConfigs)).toMap
    _ <- name.fold(client.alterTopicsConfigs(conf).tapError(err => ZIO.logError(s"$err")))(topic => client.alterTopicConfigs(topic, configs.getOrElse(Map.empty[String, String])).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Reset configs of topic(s) ${name.fold(conf.keySet.mkString(","))(topicName => s"$topicName")} unless topics didn't exist")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("set-configs", "set the given configs for the specified topic") {
    val configsOpts = Opts.option[String](long = "topicConfigs", short = "p", help = "type the topic configs according to the following pattern: key1=value1,key2=value2 etc")
      .map(_.split(",").flatMap(_.split("=")).toList.grouped(2).map { case List(a, b) => (a, b) }.toMap).map(Some(_)).withDefault(None)
    configsOpts.map(configs => (name: Option[String]) => (path: String) => app(name, configs).provide(pathLayer(path), Scope.default))
  }

}
