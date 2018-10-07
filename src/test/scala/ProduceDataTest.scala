import java.util

import ProduceData.CHOOSE_SERIALIZER
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.scalatest.BeforeAndAfterAll

import sys.process._
import scala.collection.JavaConverters._

class ProduceDataTest extends UnitSpec with BeforeAndAfterAll {

  val CHOOSE_DESERIALIZER: Map[Serializer, String] = Map[Serializer, String](
    Serializer.AVRO -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    Serializer.STRING -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  val brokers: String = "http://" + System.getProperty("broker:9093")
  val schemaRegistryUrl: String = "http://" + System.getProperty("schema_registry:8081")
  val schemaName: String = "gb.portability.activity"

  val schemaValue: String = scala.io.Source.fromURL(getClass.getResource("/AvroSchemaValue")).getLines.mkString
  val schemaKey: String = scala.io.Source.fromURL(getClass.getResource("/AvroSchemaKey")).getLines.mkString

  val curlSchemaValue = Seq("curl", "-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data",
    s"$schemaValue", schemaRegistryUrl + "/subjects/" + schemaName + "-value/versions")

  val curlSchemaKey = Seq("curl", "-X", "POST", "-H", "Content-Type: application/vnd.schemaregistry.v1+json", "--data",
    s"$schemaKey", schemaRegistryUrl + "/subjects/" + schemaName + "-key/versions")

  Thread.sleep(60000)

  curlSchemaValue.!
  curlSchemaKey.!


  "Produce avro data type with key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "true".toBoolean
    val serializerType = Serializer.AVRO

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records = RandomDataProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataProducer.produceMessages(producer, serializerType, records, topicName)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    var recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(100)
    var recordsPolledCount = recordsPolled.count()

    while(recordsPolledCount == 0) {
      recordsPolled =  consumer.poll(100)
      recordsPolledCount = recordsPolled.count()
    }
    consumer.commitSync()

    assert(recordsPolledCount == numberRecord)

    assert(recordsPolled.asScala.head.key() == records.toList.head.asInstanceOf[(Any, Any)]._1)
    assert(recordsPolled.asScala.head.value() == records.toList.head.asInstanceOf[(Any, Any)]._2)
  }

  "Produce avro data type without key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "false".toBoolean
    val serializerType = Serializer.AVRO

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records = RandomDataProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataProducer.produceMessages(producer, serializerType, records, topicName)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    var recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(100)
    var recordsPolledCount = recordsPolled.count()

    while(recordsPolledCount == 0) {
      recordsPolled =  consumer.poll(100)
      recordsPolledCount = recordsPolled.count()
    }
    consumer.commitSync()

    assert(recordsPolledCount == numberRecord)

    assert(recordsPolled.asScala.head.key() == null)
    assert(recordsPolled.asScala.head.value() == records.toList.head)
  }

  "Produce string data type without key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity"
    val numberRecord = 1
    val hasKey = "false".toBoolean
    val serializerType = Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records = RandomDataProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataProducer.produceMessages(producer, serializerType, records, topicName)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    var recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(100)
    var recordsPolledCount = recordsPolled.count()

    while(recordsPolledCount == 0) {
      recordsPolled =  consumer.poll(100)
      recordsPolledCount = recordsPolled.count()
    }
    consumer.commitSync()

    assert(recordsPolledCount == numberRecord)

    assert(recordsPolled.asScala.head.key() == null)
    assert(recordsPolled.asScala.head.value() == records.toList.head.toString)
  }

  "Produce string data type with key" should "write record in kafka topic" in {

    val topicName = "gb.portability.activity.avro"
    val numberRecord = 1
    val hasKey = "true".toBoolean
    val serializerType = Serializer.STRING

    val keyDeserializer = CHOOSE_DESERIALIZER(serializerType)
    val valueDeserializer = CHOOSE_DESERIALIZER(serializerType)

    val producer = CustomProducer(brokers, schemaRegistryUrl, CHOOSE_SERIALIZER(serializerType), CHOOSE_SERIALIZER(serializerType))

    val records = RandomDataProducer.getRecordsToWrite(schemaRegistryUrl, schemaName, numberRecord, hasKey)

    assert(records.size == numberRecord)

    RandomDataProducer.produceMessages(producer, serializerType, records, topicName)
    producer.producer.close()

    val consumer = CustomConsumer(brokers, schemaRegistryUrl, keyDeserializer, valueDeserializer).consumer

    consumer.subscribe(util.Collections.singletonList(topicName))

    var recordsPolled: ConsumerRecords[Any, Any] = consumer.poll(100)
    var recordsPolledCount = recordsPolled.count()

    while(recordsPolledCount == 0) {
      recordsPolled =  consumer.poll(100)
      recordsPolledCount = recordsPolled.count()
    }
    consumer.commitSync()

    assert(recordsPolledCount == numberRecord)

    assert(recordsPolled.asScala.head.key() == records.toList.head.asInstanceOf[(Any, Any)]._1.toString)
    assert(recordsPolled.asScala.head.value() == records.toList.head.asInstanceOf[(Any, Any)]._2.toString)
  }
}
