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

object TopicDescribe {

  def app(name: Option[String]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    description <- name
      .fold(client.describeTopicsCustom(nested.topics.map(_.topicName)).tapError(err => ZIO.logError(s"$err")))(topic => client.describeTopic(topic).map(List(_)).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(description.sortBy(_.name).map(_.mkString).mkString("\n"))
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("describe", "display the topic description") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val nameOpts = Opts.option[String](long = "topicName", short = "name", help = "type the topic name").map(Some(_)).withDefault(None)
    unsafe { implicit unsafe => (pathOpts, nameOpts).mapN((path, name) => Runtime.default.unsafe.run(app(name).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie)) }
  }

}
