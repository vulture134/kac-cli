package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.AdminClient.*
import client.AdminClientSettings
import zio.Unsafe.unsafe
import zio.{Console, ExitCode, Runtime, Scope, ZIO}

object ConsumerGroupDelete {

  def app(groupId: Option[String]): ZIO[Scope & Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- groupId
      .fold(client.deleteConsumerGroups(cfg.groups.map(_.groupId)).tapError(err => ZIO.logError(s"$err")))(group => client.deleteConsumerGroup(group).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Deleted consumer group(s) ${groupId.fold(cfg.groups.map(_.groupId).mkString(","))(group => s"$group")} if they(it) existed")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("delete", "delete a consumer group") {
    Opts((id: Option[String]) => (path: String) => app(id).provide(pathLayer(path), Scope.default))
  }

}
