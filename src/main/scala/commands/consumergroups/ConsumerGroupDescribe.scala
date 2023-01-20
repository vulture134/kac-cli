package commands.consumergroups

import cats.*
import cats.implicits.*
import com.monovore.decline.{Command, Opts}
import config.Config
import config.Config.pathLayer
import client.AdminClient.*
import client.AdminClientSettings
import zio.Unsafe.unsafe
import zio.kafka.consumer.{Consumer, ConsumerSettings}
import zio.{Console, Exit, ExitCode, Runtime, Scope, Task, Unsafe, ZIO, ZLayer}

object ConsumerGroupDescribe {

  def app(groupId: Option[String]): ZIO[Scope & Config & Consumer, Throwable, ExitCode] = for {
    cfg <- ZIO.service[Config]
    client <- make(AdminClientSettings(cfg.bootstrapServers))
    listing <- groupId
      .fold(ZIO.foreach(cfg.groups)(group => client.describeConsumerGroup(group.groupId)).tapError(err => ZIO.logError(s"$err")))(group => client.describeConsumerGroup(group).map(List(_)).tapError(err => ZIO.logError(s"$err")))
    metadata = listing.map(map => map.toList.sortBy(_._1)
      .map(rec => s"groupId: ${rec._1}\n${rec._2.toList.sortBy(tuple => (tuple._1.topic, tuple._1.partition))
        .map(data => s" topic: ${data._1.topic}, partition: ${data._1.partition}, currentOffset: ${data._2.currentOffset}, endOffset: ${data._2.endOffset}, lag: ${data._2.lag}, metadata: ${data._2.metadata}").mkString("\n")}").mkString("\n")).mkString("\n")
    _ <- Console.printLine(s"$metadata")
    _ <- client.close
  } yield ExitCode.success

  val layer: ZLayer[Scope & Config, Throwable, Consumer] = ZLayer {
    for {
      cfg <- ZIO.service[Config]
      consumer <- Consumer.make(ConsumerSettings(cfg.bootstrapServers).withGroupId("technical"))
    } yield consumer
  }

  val command: Command[Option[String] => String => ZIO[Any, Throwable, ExitCode]] = Command("describe", "describes given consumer groups") {
    Opts((id: Option[String]) => (path: String) => app(id).provide(pathLayer(path), Scope.default, layer))
  }

}
