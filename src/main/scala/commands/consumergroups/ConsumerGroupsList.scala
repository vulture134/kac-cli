package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config
import config.Config.{pathLayer, resultLayer}
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}


object ConsumerGroupsList {

  val app: ZIO[Scope with Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    listing <- client.listConsumerGroups()
    _ <- Console.printLine(listing.toList.sortBy(_.groupId).map(_.mkString).mkString("\n"))
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("list", "display existing consumer groups") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    unsafe {implicit unsafe => pathOpts.map(path => Runtime.default.unsafe.run(app.provide(resultLayer(path), Scope.default).orDie))}
  }

}
