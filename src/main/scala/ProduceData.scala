import conf.Config
import dataGenerator.RandomDataProducer
import enums.Serializer
import kafka.producers.CustomProducer
import org.slf4j.LoggerFactory
import pureconfig.error.ConfigReaderFailures

object ProduceData {
  private val logger = LoggerFactory.getLogger(ProduceData.getClass)

  val CHOOSE_SERIALIZER: Map[Serializer, String] = Map[Serializer, String](
    Serializer.AVRO -> "io.confluent.kafka.serializers.KafkaAvroSerializer",
    Serializer.STRING -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  def main(args: Array[String]): Unit = {
    //val config: Config = ConfigFactory.load().getConfig("main")

    /*
    val brokers = config.getString("brokersList")
    val schemaRegistryUrl = config.getString("schemaRegistryUrl")
    val schemaName = config.getString("schemaName")
    val topicName = config.getString("topicName")
    val numberRecord = config.getInt("numberRecord")
    val hasKey = config.getBoolean("hasKey")
    val serializerType = config.getEnum[enums.Serializer](classOf[enums.Serializer], "serializerType")

    //TODO Read json and trasform it in key value Map
    //val schemaFieldsWithCustomValue: util.List[_] = config.getAnyRefList("")
    val fieldsWithCustomValue: util.Map[String, AnyRef] = config.getObject("").unwrapped()
    //val fieldsWithCustomValue: Map[String, List[String]] = Map("providerTerritory" -> List("gb", "ie", "at", "es", "it"),
     //                               "provider" -> List("nowtv", "sky"),
     //                               "proposition" -> List("sky", "skyplus", "skyq"))
     */


    val config: Either[ConfigReaderFailures, Config] = pureconfig.loadConfig[Config]

    if (config.isLeft) {
      logger.error(s"Error during loaded config: ${config.toString}")

      throw new IllegalArgumentException("Error during loaded config")
    }

    val configLoaded = config.right.get
    logger.info(configLoaded.toString)

    CustomProducer.brokerList = configLoaded.brokersList
    CustomProducer.schemaRegistry = configLoaded.schemaRegistryUrl
    CustomProducer.keySerializer = CHOOSE_SERIALIZER(configLoaded.serializerType)
    CustomProducer.valueSerializer = CHOOSE_SERIALIZER(configLoaded.serializerType)

    val randomDataProducer = new RandomDataProducer(configLoaded.schemaRegistryUrl, configLoaded.schemaName,
      configLoaded.numberRecord, configLoaded.hasKey, configLoaded.fieldsWithCustomValue,
      configLoaded.serializerType, configLoaded.topicName)

    logger.info("PRODUCER STARTED")

    val records: Iterator[Any] = randomDataProducer.getRecordsToWrite

    try {

      randomDataProducer.produceMessages(CustomProducer.instance, records)

    }finally {
      CustomProducer.instance.producer.flush()
      CustomProducer.instance.producer.close()
    }

    logger.info("PRODUCER FINISHED")
  }

}
