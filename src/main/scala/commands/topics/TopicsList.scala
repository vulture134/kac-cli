package commands.topics

import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config
import config.Config.pathLayer
import client.*
import client.AdminClient.*
import zio.Unsafe.unsafe
import zio.{Console, Exit, ExitCode, Runtime, Scope, Unsafe, ZIO}


object TopicsList {

  def app(flag: Boolean): ZIO[Scope with Config, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    listing <- client.listTopics()
    topics = cfg.topics.map(_.topicName)
    short = topics.filter(listing.keySet.contains).sorted.mkString("\n")
    _ <- Console.printLine(if flag then listing.keySet.toList.sorted.mkString("\n") else if short.isEmpty then "None of the given topics exists" else short)
    _ <- client.close
  } yield ExitCode.success

  val command: Command[String => ZIO[Any, Throwable, ExitCode]] = Command("list", "display existing topics") {
    val allOpts = Opts.flag(long = "all", short = "a", help = "flag for displaying all topics (displays only existing topics from given topic configs otherwise)").orFalse
    allOpts.map(all => (path: String) => app(all).provide(pathLayer(path), Scope.default))
  }

}
