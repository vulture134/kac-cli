package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.AdminClient.*
import client.AdminClientSettings
import zio.Unsafe.unsafe
import zio.{Console, ExitCode, Runtime, Scope, ZIO}

object TopicDelete {

  def app(name: Option[String]): ZIO[Scope & Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- name
      .fold(client.deleteTopics(cfg.topics.map(_.topicName)).tapError(err => ZIO.logError(s"$err")))(topicName => client.deleteTopic(topicName).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Deleted topic(s) ${name.fold(cfg.topics.map(_.topicName).mkString(","))(topicName => s"$topicName")} if it(they) existed")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("delete", "delete a topic") {
    Opts((name: Option[String]) => (path: String) => app(name).provide(pathLayer(path), Scope.default))
  }

}
