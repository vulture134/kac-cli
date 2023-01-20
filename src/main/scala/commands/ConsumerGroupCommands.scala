package commands

import com.monovore.decline.*
import commands.*
import com.monovore.decline.Opts.alternative.*
import commands.consumergroups.*
import cats.implicits.*
import zio.*
import consumergroups.*

object ConsumerGroupCommands {

  val idOpts: Opts[Option[String]] = Opts.option[String](long = "groupId", short = "g", help = "type the consumer group id").map(Some(_)).withDefault(None)
  val ids = Opts.subcommands[Option[String] => String => ZIO[Any, Throwable, ExitCode]](
    ConsumerGroupAlterOffsets.command,
    ConsumerGroupDelete.command,
    ConsumerGroupOffsetsList.command,
    ConsumerGroupsDeleteOffsets.command,
    ConsumerGroupSetOffsetsToTimestamp.command,
    ConsumerGroupDescribe.command
  )
  val subcommandsWithId = ap[Option[String], String => ZIO[Any, Throwable, ExitCode]](ids)(idOpts)
  val subcommandList = Opts.subcommands[String => ZIO[Any, Throwable, ExitCode]](
    ConsumerGroupsList.command
  )
  val commands = Command("groups", "commands for consumer groups management")(combineK(subcommandsWithId, subcommandList))

}
