package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}

object TopicDescribe {

  def app(name: Option[String]): ZIO[Scope & Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    description <- name
      .fold(client.describeTopicsCustom(cfg.topics.map(_.topicName)).tapError(err => ZIO.logError(s"$err")))(topic => client.describeTopic(topic).map(List(_)).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(description.sortBy(_.name).map(_.mkString).mkString("\n"))
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("describe", "display the topic description") {
    Opts((name: Option[String]) => (path: String) => app(name).provide(pathLayer(path), Scope.default))
  }

}
