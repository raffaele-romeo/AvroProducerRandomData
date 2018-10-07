import org.slf4j.LoggerFactory

object RandomDataProducer {
  private val logger = LoggerFactory.getLogger(RandomDataProducer.getClass)

  def produceMessages(producer: CustomProducer, serializerType: Serializer, records: Iterator[Any], topic: String): Unit = {

    serializerType match {
      case Serializer.STRING =>
        records.foreach {
          case (key, value) => producer.send(topic, key.toString, value.toString)
          case value => producer.send(topic, value.toString)
        }
      case Serializer.AVRO =>
        records.foreach {
          case (key, value) => producer.send(topic, key, value)
          case value => producer.send(topic, value)
        }
      case _ =>
        throw new Exception("Type of serialization not valid")

    }

    producer.producer.flush()
  }

  def getRecordsToWrite(schemaRegistryUrl: String, schemaName: String, numberRecord: Int, hasKey: Boolean): Iterator[Any] = {

    if (hasKey) {

      val keys = getRandomDataBasedOnSchema(schemaRegistryUrl, schemaName + "-key", numberRecord)
      val values = getRandomDataBasedOnSchema(schemaRegistryUrl, schemaName + "-value", numberRecord)
      keys zip values

    } else {

      getRandomDataBasedOnSchema(schemaRegistryUrl, schemaName + "-value", numberRecord)
    }

  }

  private def getRandomDataBasedOnSchema(schemaRegistryUrl: String, schemaName: String, numberRecord: Int): Iterator[Any] = {

    lazy val schema = SchemaRegistry.retrieveSchema(schemaRegistryUrl, schemaName)
    val randomData = new RandomData(schema, numberRecord)

    randomData.iterator
  }
}
