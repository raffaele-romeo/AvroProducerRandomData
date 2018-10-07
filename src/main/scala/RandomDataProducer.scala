import org.apache.avro._
import org.slf4j.LoggerFactory

case class RandomDataProducer(producer: CustomProducer, serializerType: Serializer, hasKey: Boolean,
                         schemaRegistryUrl: String, schemaName: String, topic: String) {

  private val randomData = new RandomData()
  private val schemaValue: Schema  = SchemaRegistry.retrieveSchema(schemaRegistryUrl, schemaName + "-value")
  private var schemaKey: Schema = _

  if(hasKey)
    schemaKey = SchemaRegistry.retrieveSchema(schemaRegistryUrl, schemaName + "-key")

  def produce(record: Any): Unit = {

    serializerType match {
      case Serializer.STRING =>
        record match {
          case (key, value) =>
            producer.send(topic, key.toString, value.toString)
          case value =>
            producer.send(topic, value.toString)
        }
      case Serializer.AVRO =>
        record match {
          case (key, value) =>
            producer.send(topic, key, value)
          case value =>
            producer.send(topic, value)
        }
      case _ =>
        throw new Exception("Type of serialization not valid")

    }

    producer.producer.flush()
  }

  def generateRecord: Any = {
    if (hasKey)
      (randomData.generate(schemaKey), randomData.generate(schemaValue))
    else
      randomData.generate(schemaValue)
  }


}

object RandomDataProducer {

  private val logger = LoggerFactory.getLogger(RandomDataProducer.getClass)

}