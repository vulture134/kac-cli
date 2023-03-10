package config

import client.AdminClient.NewTopic
import pureconfig.*
import pureconfig.generic.derivation.default.*
import zio.*

case class Config (bootstrapServers: List[String], topics: List[TopicConfig], groups: List[ConsumerGroupConfig]) derives ConfigReader

case class TopicConfig (topicName: String, numPartitions: Int, replicationFactor: Short, topicConfigs: Map[String, String]) derives ConfigReader
{
  val newTopic: NewTopic = NewTopic(topicName, Some(numPartitions), Some(java.lang.Short.valueOf(replicationFactor)), topicConfigs)
}

case class ConsumerGroupConfig(groupId: String, topicName: String, offsets: Map[String, String], date: String) derives ConfigReader

object Config {

  val layer: ZLayer[Any, IllegalStateException, Config] = ZLayer {
    ZIO
      .fromEither(ConfigSource.default.load[Config])
      .mapError(
        failures =>
          new IllegalStateException(
            s"Error loading configuration: $failures"
          )
      )
  }

  def pathLayer(path: String): ZLayer[Any, IllegalStateException, Config] = ZLayer {
    ZIO
      .fromEither(ConfigSource.file(path).load[Config])
      .mapError(
        failures =>
          new IllegalStateException(
            s"Error loading configuration: $failures"
          )
      )
  }

  def resultLayer(path: Option[String] = None): ZLayer[Any, IllegalStateException, Config] = path.fold(layer)(pathLayer)

}


