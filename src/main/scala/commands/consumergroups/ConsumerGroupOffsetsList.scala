package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.pathLayer
import config.Config
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Task, Unsafe, ZIO}

object ConsumerGroupOffsetsList {

  def app(groupId: Option[String]): ZIO[Scope & Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    listing <- groupId
      .fold(client.listConsumerGroupsOffsets(cfg.groups.map(_.groupId)).tapError(err => ZIO.logError(s"$err")))(group => client.listConsumerGroupOffsets(group).tapError(err => ZIO.logError(s"$err")))
    metadata = listing.toList.sortBy(_._1).map(rec => s"groupId: ${rec._1}\n${rec._2.toList.sortBy(_._1.partition).map(data => s" topic: ${data._1.topic}, partition: ${data._1.partition}, offset: ${data._2.offset}, metadata: ${data._2.metadata}").mkString("\n")}").mkString("\n")
    _ <- Console.printLine(s"$metadata")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("offsets", "display partition offsets for existing consumer group") {
    Opts((id: Option[String]) => (path: String) => app(id).provide(pathLayer(path), Scope.default))
  }

}
