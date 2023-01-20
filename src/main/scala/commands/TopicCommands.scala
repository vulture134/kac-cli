package commands

import com.monovore.decline.*
import commands.*
import com.monovore.decline.Opts.alternative.*
import commands.consumergroups.*
import cats.implicits.*
import zio.*
import topics.*

object TopicCommands {

  val nameOpts = Opts.option[String](long = "topic", short = "t", help = "type the topic name").map(Some(_)).withDefault(None)
  val names = Opts.subcommands[Option[String] => String => zio.ZIO[Any, Throwable, zio.ExitCode]](
    TopicCreate.command,
    TopicDelete.command,
    TopicDescribe.command,
    TopicSetConfigs.command,
  )
  val subcommandsWithNames = ap[Option[String], String => ZIO[Any, Throwable, ExitCode]](names)(nameOpts)
  val subcommandList = Opts.subcommands[String => ZIO[Any, Throwable, ExitCode]](
    TopicsList.command
  )
  val commands = Command("topics", "commands for topics management")(combineK(subcommandsWithNames, subcommandList))

}
