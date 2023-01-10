import com.monovore.decline.*
import commands.*

object Main extends CommandApp(
  name = "kafka-admin-client-cli",
  header = "Command line interface for running kafka admin client tasks",
  main = {
    Opts.subcommands(
     ConsumerGroupCommands.commands,
     TopicCommands.commands
    )
  }
)

