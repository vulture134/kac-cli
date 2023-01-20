package commands.consumergroups

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config
import config.Config.pathLayer
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}


object ConsumerGroupsList {

  def app(flag: Boolean): ZIO[Scope with Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    listing <- client.listConsumerGroups()
    groups = cfg.groups.map(_.groupId)
    short = listing.filter(list => groups.contains(list.groupId)).toList.sortBy(_.groupId).map(_.mkString).mkString("\n")
    _ <- Console.printLine(if flag then listing.toList.sortBy(_.groupId).map(_.mkString).mkString("\n") else if short.isEmpty then "None of the given consumer groups exists" else short)
    _ <- client.close
  } yield ExitCode.success

  val command: Command[String => ZIO[Any, Throwable, ExitCode]] = Command("list", "display existing consumer groups") {
    val allOpts = Opts.flag(long = "all", short = "a", help = "flag for displaying all topics (displays only existing topics from given topic configs otherwise)").orFalse
    allOpts.map(all => (path: String) => app(all).provide(pathLayer(path), Scope.default))
  }

}
