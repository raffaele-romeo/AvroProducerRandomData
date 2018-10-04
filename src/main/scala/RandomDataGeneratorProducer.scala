import org.apache.avro._

import scala.util.Random

object RandomDataGeneratorProducer {

  def produce(randomData: Iterator[Any], producer: CustomProducer,
              topic: String, hasKey: Boolean): Unit = {
    while( randomData.hasNext ) {
      if(hasKey){
        val random = new Random()
        producer.send(topic, random.nextInt().toString, randomData.next())
      }else {
        producer.send(topic, randomData.next())
      }
    }
  }
  }
