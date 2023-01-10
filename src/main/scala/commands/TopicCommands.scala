package commands

import com.monovore.decline.{Command, Opts}
import commands.topics.*

object TopicCommands {
  val commands: Command[Unit] = Command("topics", "commands for topic management")(
    Opts.subcommands(
      TopicsList.command,
      TopicCreate.command,
      TopicDelete.command,
      TopicDescribe.command,
      TopicSetConfigs.command
    )
  )
}
