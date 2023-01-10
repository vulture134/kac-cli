package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config.resultLayer
import config.NestedConfig.nestedConfigLayer
import config.{Config, NestedConfig}
import client.AdminClient.*
import client.AdminClientSettings
import zio.Unsafe.unsafe
import zio.{Console, ExitCode, Runtime, Scope, ZIO}

object ConsumerGroupDelete {

  def app(groupId: Option[String]): ZIO[Scope & Config & NestedConfig, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    nested <- ZIO.service[NestedConfig]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    _ <- groupId
      .fold(client.deleteConsumerGroups(nested.groups.map(_.groupId)).tapError(err => ZIO.logError(s"$err")))(group => client.deleteConsumerGroup(group).tapError(err => ZIO.logError(s"$err")))
    _ <- Console.printLine(s"Deleted consumer group(s) ${groupId.fold(nested.groups.map(_.groupId).mkString(","))(group => s"$group")} if they(it) existed")
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("delete", "delete a consumer group") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    val idOpts = Opts.option[String](long = "groupId", short = "id", help = "type the consumer group id").map(Some(_)).withDefault(None)
    unsafe { implicit unsafe => (pathOpts, idOpts).mapN((path, id) => Runtime.default.unsafe.run(app(id).provide(resultLayer(path), Scope.default, nestedConfigLayer).orDie)) }
  }

}
