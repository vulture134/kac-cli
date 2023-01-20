import com.monovore.decline.*
import commands.*
import zio.Unsafe.unsafe
import com.monovore.decline.Opts.alternative.*
import commands.consumergroups.*
import cats.implicits.*
import zio.*

object Main extends CommandApp(
  name = "kafka-admin-client-cli",
  header = "Command line interface for running kafka admin client tasks",
  main = {
    val pathOpts: Opts[String] = Opts.option[String](long = "configPath", short = "c", help = "type path to configs file")
      .withDefault(s"${java.lang.System.getProperty("user.dir")}/${sys.env.getOrElse("KAC_CONF_FILE", "kac.conf")}")
    val commands = Opts.subcommands[String => zio.ZIO[Any, Throwable, zio.ExitCode]](
      ConsumerGroupCommands.commands,
      TopicCommands.commands
    )

    val app = ap[String, ZIO[Any, Throwable, ExitCode]](commands)(pathOpts)

    app.map(program => unsafe {implicit unsafe => Runtime.default.unsafe.run(program).getOrThrowFiberFailure() } )

  }
)

