package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.{pathLayer, resultLayer}
import config.NestedConfig.nestedConfigLayer
import config.{Config, NestedConfig}
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Task, Unsafe, ZIO}

object ConsumerGroupOffsetsList {

  def app(groupId: Option[String]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    listing <- groupId
      .fold(client.listConsumerGroupsOffsets(nested.groups.map(_.groupId)).tapError(err => ZIO.logError(s"$err")))(group => client.listConsumerGroupOffsets(group).tapError(err => ZIO.logError(s"$err")))
    metadata = listing.toList.sortBy(_._1).map(rec => s"groupId: ${rec._1}\n${rec._2.toList.sortBy(_._1.partition).map(data => s" topic: ${data._1.topic}, partition: ${data._1.partition}, offset: ${data._2.offset}, metadata: ${data._2.metadata}").mkString("\n")}").mkString("\n")
    _ <- Console.printLine(s"$metadata")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("offsets", "display partition offsets for existing consumer group") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val idOpts = Opts.option[String](long = "groupId", short = "Id", help = "type the consumer group id").map(Some(_)).withDefault(None)
    unsafe { implicit unsafe => (pathOpts, idOpts).mapN((path, id) => Runtime.default.unsafe.run(app(id).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie)) }
  }

}
