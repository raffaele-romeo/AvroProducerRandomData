import java.util

import ProduceData.{CHOOSE_SERIALIZER, logger}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.BeforeAndAfterAll

import scala.collection.JavaConverters._

import scala.sys.process._

class ProduceDataTest extends UnitSpec with BeforeAndAfterAll{

  val CHOOSE_DESERIALIZER: Map[Serializer, String] = Map[Serializer, String](
    Serializer.AVRO -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    Serializer.STRING -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  val brokers = "http://172.18.0.1:9093" //+ System.getProperty("broker:9093")
  val schemaRegistryUrl = "http://172.18.0.1:8081" //+ System.getProperty("schema_registry:8081")
  val schemaName = "gb.portability.activity"
  val topicName = "gb.portability.activity"


  override def beforeAll(): Unit = {
    super.beforeAll()
    val json = "'" + scala.io.Source.fromResource("AvroSchema").getLines.mkString + "'"

    val cmd = Seq("curl", "-X", "POST", "-H", """ "Content-Type: application/vnd.schemaregistry.v1+json" """, "--data",
      s"$json",  schemaRegistryUrl + "/subjects/" + schemaName + "-value/versions")

    cmd.!
  }

  "Produce string data type without key" should "write record in kafka topic" in {

    val numberRecord = 1
    val hasKey = "false".toBoolean
    val serializerType = Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    CustomProducer.brokerList = brokers
    CustomProducer.schemaRegistry = schemaRegistryUrl
    CustomProducer.keySerializer = CHOOSE_SERIALIZER(serializerType)
    CustomProducer.valueSerializer = CHOOSE_SERIALIZER(serializerType)

    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    RandomDataGeneratorProducer.produceMasseges(CustomProducer.instance, serializerType, records, topicName)

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled = consumer.poll(100)

    for (record: ConsumerRecord[Any, Any] <- recordsPolled.asScala) {
        println(record.value())
    }
  }


  "Produce data" should "publish random data on kafka" in {

    val numberRecord = 10
    val hasKey = "false".toBoolean
    val serializerType = Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    CustomProducer.brokerList = brokers
    CustomProducer.schemaRegistry = schemaRegistryUrl
    CustomProducer.keySerializer = CHOOSE_SERIALIZER(serializerType)
    CustomProducer.valueSerializer = CHOOSE_SERIALIZER(serializerType)


    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    RandomDataGeneratorProducer.produceMasseges(CustomProducer.instance, serializerType, records, topicName)

  }

  }
