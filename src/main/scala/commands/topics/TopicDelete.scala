package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.resultLayer
import config.NestedConfig.nestedConfigLayer
import config.{Config, NestedConfig}
import client.AdminClient.*
import client.AdminClientSettings
import zio.Unsafe.unsafe
import zio.{Console, ExitCode, Runtime, Scope, ZIO}

object TopicDelete {

  def app(name: Option[String]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- name
      .fold(client.deleteTopics(nested.topics.map(_.topicName)).tapError(err => ZIO.logError(s"$err")))(topicName => client.deleteTopic(topicName).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Deleted topic(s) ${name.fold(nested.topics.map(_.topicName).mkString(","))(topicName => s"$topicName")} if it(they) existed")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("delete", "delete a topic") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val nameOpts = Opts.option[String](long = "topicName", short = "name", help = "type the topic name").map(Some(_)).withDefault(None)
    unsafe {implicit unsafe => (pathOpts, nameOpts).mapN((path, name) => Runtime.default.unsafe.run(app(name).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie))}
  }

}
