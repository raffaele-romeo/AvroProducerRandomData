import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import SerializerType._

object ProduceData {
  private val logger = LoggerFactory.getLogger(ProduceData.getClass)


  val CHOOSE_SERIALIZER: Map[SerializerType.Serializer, String] = Map[SerializerType.Serializer, String](
    SerializerType.AVRO -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
    SerializerType.STRING -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  def main(args: Array[String]): Unit = {
    //TODO TEST

    val config: Config = ConfigFactory.load().getConfig("main")

    val brokers = config.getString("brokersList")
    val schemaRegistryUrl = config.getString("schemaRegistryUrl")
    val schemaName = config.getString("schemaName")
    val topicName = config.getString("topicName")
    val numberRecord = config.getInt("numberRecord")
    val hasKey = config.getBoolean("hasKey")
    val serializerType: SerializerType.Serializer = config.getEnum[Serializer](classOf[Serializer], "serializerType")


    CustomProducer.brokerList = brokers
    CustomProducer.schemaRegistry = schemaRegistryUrl
    CustomProducer.keySerializer = CHOOSE_SERIALIZER(serializerType)
    CustomProducer.valueSerializer = CHOOSE_SERIALIZER(serializerType)

    logger.info("PRODUCER STARTED")

    RandomDataGeneratorProducer.produce(schemaRegistryUrl, schemaName, numberRecord, CustomProducer.instance,
      topicName, hasKey, serializerType)

    logger.info("PRODUCER FINISHED")
  }

}
