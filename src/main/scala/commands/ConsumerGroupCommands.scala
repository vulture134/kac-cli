package commands

import com.monovore.decline.{Command, Opts}
import commands.*
import commands.consumergroups.*

object ConsumerGroupCommands {
  val commands: Command[Unit] = Command("consumer-groups", "commands for consumer group management")(
    Opts.subcommands(
      ConsumerGroupsList.command,
      ConsumerGroupOffsetsList.command,
      ConsumerGroupAlterOffsets.command,
      ConsumerGroupDelete.command,
      ConsumerGroupsDeleteOffsets.command,
      ConsumerGroupSetOffsetsToTimestamp.command))
}
