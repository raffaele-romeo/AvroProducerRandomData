import org.apache.avro._
import org.slf4j.LoggerFactory

object RandomDataGeneratorProducer {
  private val logger = LoggerFactory.getLogger(RandomDataGeneratorProducer.getClass)

  def produce(schemaRegistryUrl: String, schemaName: String, numberRecord: Int, producer: CustomProducer,
              topic: String, hasKey: Boolean, serializerType: Serializer): Unit = {

    if (hasKey)
      produceKeyValueMasseges(producer, serializerType, schemaRegistryUrl, schemaName, numberRecord, topic)
    else
      produceValueMasseges(producer, serializerType, schemaRegistryUrl, schemaName, numberRecord, topic)

    producer.producer.flush()
  }


  private def produceValueMasseges(producer: CustomProducer, serializerType: Serializer, schemaRegistryUrl: String,
                                   schemaName: String, numberRecord: Int, topic: String): Unit = {

    val values = getRandomData(schemaRegistryUrl, schemaName + "-value", numberRecord).toList

    serializerType match {
      case Serializer.STRING =>
        values.foreach(value => producer.send(topic, value.toString))
      case Serializer.AVRO =>
        values.foreach(value => producer.send(topic, value))
      case _ =>
        throw new Exception("Type of serialization not valid")
    }
  }

  private def produceKeyValueMasseges(producer: CustomProducer, serializerType: Serializer, schemaRegistryUrl: String,
                                      schemaName: String, numberRecord: Int, topic: String): Unit = {

    val keys = getRandomData(schemaRegistryUrl, schemaName + "-key", numberRecord).toList
    val values = getRandomData(schemaRegistryUrl, schemaName + "-value", numberRecord).toList
    val keysValues = keys zip values

    serializerType match {
      case Serializer.STRING =>
        keysValues.foreach {
          case (key, value) => producer.send(topic, key.toString, value.toString)
        }
      case Serializer.AVRO =>
        keysValues.foreach {
          case (key, value) => producer.send(topic, key, value)
        }
      case _ =>
        throw new Exception("Type of serialization not valid")

    }
  }

  private def getRandomData(schemaRegistryUrl: String, schemaName: String, numberRecord: Int): Iterator[Any] = {

    try {
      val schema = SchemaRegistry.retrieveSchema(schemaRegistryUrl, schemaName)
      val randomData = new RandomData(schema, numberRecord)

      randomData.iterator
    } catch {
      case e: Exception =>
        val DEFAULTSCHEMA$: Schema = (new Schema.Parser).parse(""" ""string"" """)
        logger.warn( s"No schema on the schema registry. We're using default schema: ${DEFAULTSCHEMA$.toString}" )

        val randomData = new RandomData(DEFAULTSCHEMA$, numberRecord)
        randomData.iterator
    }
  }
}
