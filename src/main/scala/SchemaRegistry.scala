import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

object SchemaRegistry {
  private val logger = LoggerFactory.getLogger(SchemaRegistry.getClass)

  private val cache = TrieMap[String, Schema]()

  private def createSchemaRegistry(url: String, maxSize: Int = 1000) = {
    new CachedSchemaRegistryClient(url, maxSize)
  }

  def retrieveSchema(url: String, schemaName: String): Schema = {

    cache.get(schemaName) match {
      case Some(schema) => schema
      case None =>
        synchronized {
          cache.get(schemaName) match {
            case Some(schema) => schema
            case None => {
              try {

                val schemaRegistry = createSchemaRegistry(url)
                val schemaMetadata = schemaRegistry.getLatestSchemaMetadata(schemaName)
                val getSchema = schemaRegistry.getById(schemaMetadata.getId)
                cache.put(schemaName, getSchema)
                getSchema

              } catch {
                case e: Exception =>
                  //val DEFAULTSCHEMA$: Schema = (new Schema.Parser).parse( """ {"type": "string"} """ )
                  //logger.warn( s"No schema on the schema registry. We're using default schema: ${DEFAULTSCHEMA$.toString}" )

                  //val randomData = new RandomData(DEFAULTSCHEMA$, numberRecord)
                  //randomData.iterator
                  logger.error("Not found schema on the schema registry.")
                  throw e
              }
            }
          }
        }
    }
  }
}
