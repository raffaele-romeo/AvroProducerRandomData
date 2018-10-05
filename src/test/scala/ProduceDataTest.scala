import java.util

import ProduceData.CHOOSE_SERIALIZER
import org.scalatest.BeforeAndAfterAll

import sys.process._

import scala.collection.JavaConverters._

class ProduceDataTest extends UnitSpec with BeforeAndAfterAll {

  val CHOOSE_DESERIALIZER: Map[Serializer, String] = Map[Serializer, String](
    Serializer.AVRO -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    Serializer.STRING -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  val brokers = "http://" + System.getProperty("broker:9093")
  val schemaRegistryUrl = "http://" + System.getProperty("schema_registry:8081")
  val schemaName = "gb.portability.activity"

  val schemaValue = scala.io.Source.fromResource("AvroSchemaValue").getLines.mkString
  val schemaKey = scala.io.Source.fromResource("AvroSchemaKey").getLines.mkString

  val curlSchemaValue = Seq("curl", "-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data",
    s"$schemaValue", schemaRegistryUrl + "/subjects/" + schemaName + "-value/versions")

  val curlSchemaKey = Seq("curl", "-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data",
    s"$schemaKey", schemaRegistryUrl + "/subjects/" + schemaName + "-key/versions")

  Thread.sleep(30000)

  curlSchemaValue.!
  curlSchemaKey.!


  "Produce avro data type with key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "true".toBoolean
    val serializerType = Serializer.AVRO

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    CustomProducer.brokerList = brokers
    CustomProducer.schemaRegistry = schemaRegistryUrl
    CustomProducer.keySerializer = CHOOSE_SERIALIZER(serializerType)
    CustomProducer.valueSerializer = CHOOSE_SERIALIZER(serializerType)

    val producer = CustomProducer.instance

    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataGeneratorProducer.produceMasseges(producer, serializerType, records, topicName)
    //producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled = consumer.poll(1000)
    consumer.commitSync()

    assert(recordsPolled.count() == numberRecord)

    assert(recordsPolled.asScala.head.key() == records.head.asInstanceOf[(Any, Any)]._1)
    assert(recordsPolled.asScala.head.value() == records.head.asInstanceOf[(Any, Any)]._2)
  }

  "Produce avro data type without key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "false".toBoolean
    val serializerType = Serializer.AVRO

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataGeneratorProducer.produceMasseges(producer, serializerType, records, topicName)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled = consumer.poll(1000)
    consumer.commitSync()

    assert(recordsPolled.count() == numberRecord)

    assert(recordsPolled.asScala.head.key() == null)
    assert(recordsPolled.asScala.head.value() == records.head)
  }

  "Produce string data type without key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity"
    val numberRecord = 1
    val hasKey = "false".toBoolean
    val serializerType = Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataGeneratorProducer.produceMasseges(producer, serializerType, records, topicName)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled = consumer.poll(1000)
    consumer.commitSync()

    assert(recordsPolled.count() == numberRecord)

    assert(recordsPolled.asScala.head.key() == null)
    assert(recordsPolled.asScala.head.value() == records.head.toString)
  }

  "Produce string data type with key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "true".toBoolean
    val serializerType = Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataGeneratorProducer.produceMasseges(producer, serializerType, records, topicName)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    val recordsPolled = consumer.poll(1000)
    consumer.commitSync()

    assert(recordsPolled.count() == numberRecord)

    assert(recordsPolled.asScala.head.key() == records.head.asInstanceOf[(Any, Any)]._1.toString)
    assert(recordsPolled.asScala.head.value() == records.head.asInstanceOf[(Any, Any)]._2.toString)
  }


  "Produce a lot of data" should "publish random data on kafka" in {

    val topicName = "gb.portability.activity"
    val numberRecord = 100
    val hasKey = "false".toBoolean
    val serializerType = Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records: Seq[Any] = RandomDataGeneratorProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    RandomDataGeneratorProducer.produceMasseges(producer, serializerType, records, topicName)
    producer.producer.close()

  }

}
