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

object TopicSetConfigs {

  def app(name: Option[String], configs: Option[Map[String, String]]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    conf = nested.inputs.map(cfg => (cfg.topicName, cfg.topicConfigs)).toMap
    _ <- name.fold(client.alterTopicsConfigs(conf).tapError(err => ZIO.logError(s"$err")))(topic => client.alterTopicConfigs(topic, configs.getOrElse(Map.empty[String, String])).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Reset configs of topic(s) ${name.fold(conf.keySet.mkString(","))(topicName => s"$topicName")} unless topics didn't exist")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("set-configs", "set the given configs for the specified topic") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val nameOpts = Opts.option[String](long = "topicName", short = "name", help = "type the topic name").map(Some(_)).withDefault(None)
    val configsOpts = Opts.option[String](long = "topicConfigs", short = "configs", help = "type the topic configs according to the following pattern: key1=value1,key2=value2 etc")
      .map(_.split(",").flatMap(_.split("=")).toList.grouped(2).map { case List(a, b) => (a, b) }.toMap).map(Some(_)).withDefault(None)
    unsafe { implicit unsafe => (pathOpts, nameOpts, configsOpts).mapN((path, name, configs) => Runtime.default.unsafe.run(app(name, configs).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie)) }
  }

}
