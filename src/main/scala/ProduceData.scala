import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

object ProduceData {
  private val logger = LoggerFactory.getLogger(ProduceData.getClass)


  val CHOOSE_SERIALIZER: Map[Serializer, String] = Map[Serializer, String](
    Serializer.AVRO -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
    Serializer.STRING -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  def main(args: Array[String]): Unit = {
    val config: Config = ConfigFactory.load().getConfig("main")

    val brokers = config.getString("brokersList")
    val schemaRegistryUrl = config.getString("schemaRegistryUrl")
    val schemaName = config.getString("schemaName")
    val topicName = config.getString("topicName")
    val numberRecord = config.getInt("numberRecord")
    val hasKey = config.getBoolean("hasKey")
    val serializerType = config.getEnum[Serializer](classOf[Serializer], "serializerType")


    CustomProducer.brokerList = brokers
    CustomProducer.schemaRegistry = schemaRegistryUrl
    CustomProducer.keySerializer = CHOOSE_SERIALIZER(serializerType)
    CustomProducer.valueSerializer = CHOOSE_SERIALIZER(serializerType)

    logger.info("PRODUCER STARTED")


    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    RandomDataGeneratorProducer.produceMasseges(CustomProducer.instance,serializerType, records, topicName)

    logger.info("PRODUCER FINISHED")
  }

}
