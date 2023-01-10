package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config
import config.Config.{pathLayer, resultLayer}
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}


object TopicsList {

  val app: ZIO[Scope with Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    listing <- client.listTopics()
    _ <- Console.printLine(listing.keySet.toList.sorted.mkString("\n"))
    _ <- client.close
  } yield ExitCode.success

  val command: Command[Unit] = Command("list", "display existing topics") {
    val pathOpts = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file").map(Some(_)).withDefault(None)
    unsafe {implicit unsafe => pathOpts.map(path => Runtime.default.unsafe.run(app.provide(resultLayer(path), Scope.default).orDie))}
  }

}
