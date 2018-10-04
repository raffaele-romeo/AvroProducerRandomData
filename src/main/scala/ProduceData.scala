import org.apache.avro.Schema

object ProduceData {
  val SCHEMA$ =
    (new Schema.Parser).parse(
      """
        |{ "type":"record", "name":"AcImsProfile_modified", "namespace":"com.sky.dap.model", "fields":[ { "name":"origin", "type":"string" }, { "name":"activityType", "type":"string" }, { "name":"event", "type":"string" }, { "name":"householdId", "type":"string" }, { "name":"ingestionTimestamp", "type":"long" }, { "name":"originatingSystem", "type":"string" }, { "name":"proposition", "type":"string" }, { "name":"provider", "type":"string" }, { "name":"activityTimestamp", "type":"long" }, { "name":"profileid", "type":"string" }, { "name":"providerTerritory", "type":"string" }, { "name":"data", "type":{ "name":"data", "type":"record", "fields":[ { "name":"profile", "type":{ "name":"profile", "type":"record", "fields":[ { "name":"mobilenumber", "type":[ "null", "string" ], "default":null }, { "name":"targetedoptin", "type":"boolean" }, { "name":"targetedoptindate", "type":[ "null", "long" ], "default":null }, { "name":"targetedoptoutdate", "type":[ "null", "long" ], "default":null }, { "name":"securityquestionid", "type":[ "null", "string" ], "default":null }, { "name":"services", "type":{ "name":"services", "type":"record", "fields":[ { "name":"skyemail", "type":[ "null", { "name":"skyemail", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"mobiletv", "type":[ "null", { "name":"mobiletv", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"skykids", "type":[ "null", { "name":"skykids", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"nowtv", "type":[ "null", { "name":"nowtv", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"remoterecord", "type":[ "null", { "name":"remoterecord", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"sports", "type":[ "null", { "name":"sports", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"skygo", "type":[ "null", { "name":"skygo", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"nowtv_ie", "type":["null",{ "name":"nowtv_ie", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] }], "default":null }, { "name":"helpforum", "type":[ "null", { "name":"helpforum", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"skywifi", "type":[ "null", { "name":"skywifi", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"skystore", "type":[ "null", { "name":"skystore", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"community", "type":[ "null", { "name":"community", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"skylocal", "type":[ "null", { "name":"skylocal", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"sky1", "type":[ "null", { "name":"sky1", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"rewardsreviews", "type":[ "null", { "name":"rewardsreviews", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"news", "type":[ "null", { "name":"news", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null }, { "name":"accountmanagement", "type":[ "null", { "name":"accountmanagement", "type":"record", "fields":[ { "name":"start", "type":[ "null", "long" ], "default":null }, { "name":"end", "type":[ "null", "long" ], "default":null }, { "name":"globalaccess", "type":[ "null", "long" ], "default":null }, { "name":"mailbox", "type":[ "null", "string" ], "default":null }, { "name":"name", "type":[ "null", "string" ], "default":null }, { "name":"suspended", "type":[ "null", "boolean" ], "default":null }, { "name":"fullysignedup", "type":[ "null", "string" ], "default":null } ] } ], "default":null } ] } }, { "name":"profileid", "type":"string" }, { "name":"mobilenumberverified", "type":[ "null", "boolean" ], "default":null }, { "name":"title", "type":[ "null", "string" ], "default":null }, { "name":"passwordrequireschange", "type":"boolean" }, { "name":"contactemail", "type":[ "null", "string" ], "default":null }, { "name":"dateofbirth", "type":[ "null", "long" ], "default":null }, { "name":"lastname", "type":"string" }, { "name":"nsprofileid", "type":"string" }, { "name":"email", "type":[ "null", "string" ], "default":null }, { "name":"username", "type":"string" }, { "name":"hhusertype", "type":"string" }, { "name":"displayname", "type":[ "null", "string" ], "default":null }, { "name":"firstname", "type":"string" }, { "name":"skyoptin", "type":"boolean" }, { "name":"emailverified", "type":[ "null", "boolean" ], "default":null }, { "name":"termsandconditionsaccepted", "type":"boolean" }, { "name":"trackingid", "type":[ "null", "string" ], "default":null }, { "name":"hhuserauthorised", "type":[ "null", "boolean" ], "default":null }, { "name":"emailchanged", "type":[ "null", "long" ], "default":null }, { "name":"mobilenumberchanged", "type":[ "null", "long" ], "default":null }, { "name":"registrationdate", "type":"long" }, { "name":"hhid", "type":[ "null", "string" ], "default":null }, { "name":"euportability", "type":"boolean" } ] } }, { "name":"changes", "type":{ "type":"array", "items":"string" } }, { "name":"primaryprofile", "type":[ "null", "string" ], "default":null }, { "name":"servicename", "type":[ "null", "string" ], "default":null }, { "name":"baseurl", "type":[ "null", "string" ], "default":null }, { "name":"singleusetoken", "type":[ "null", "string" ], "default":null }, { "name":"securityquestionid", "type":[ "null", "string" ], "default":null } ] } }, { "name":"id", "type":"string" } ] }
      """.stripMargin)

  val CHOOSE_SERIALIZER: Map[String, String] = Map[String, String](
    "avro" -> "io.confluent.kafka.serializers.KafkaStringSerializer",
    "string" -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  def main(args: Array[String]): Unit = {
    //TODO gestire le eccezioni, anche quelle dovute all'utente
    val brokers = args(0)
    val schemaRegistryUrl = args(1)
    val schemaName = args(2)
    val topicName = args(3)
    val numberRecord: Int = args(4).toInt
    val hasKey = args(5).toBoolean
    val keySerializer = CHOOSE_SERIALIZER(args(6))
    val valueSerializer = CHOOSE_SERIALIZER(args(7))


    val schema = SchemaRegistry.retrieveSchema(schemaRegistryUrl, schemaName)
    val randomData = new RandomData(schema, numberRecord)
    val randomDataIterator = randomData.iterator

    CustomProducer.brokerList = brokers
    CustomProducer.schemaRegistry = schemaRegistryUrl
    CustomProducer.keySerializer = keySerializer
    CustomProducer.valueSerializer = valueSerializer
    RandomDataGeneratorProducer.produce(randomDataIterator, CustomProducer.instance, topicName, hasKey)
  }

}