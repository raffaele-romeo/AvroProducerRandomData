import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema

object SchemaRegistry {

  def retrieveSchema(url: String, schemaName: String): Schema = {

    val schemaRegistry = new CachedSchemaRegistryClient(url, 100)
    val schemaMetadata = schemaRegistry.getLatestSchemaMetadata(schemaName)
    val schema = schemaRegistry.getById(schemaMetadata.getId)

    schema
  }

}
