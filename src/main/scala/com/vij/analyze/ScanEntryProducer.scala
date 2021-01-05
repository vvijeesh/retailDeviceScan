package com.vij.analyze

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import scala.io.Source
import java.util.Properties

class ScanEntryProducer() extends App {

  //Default Constructor
  def ScanEntryProducer(): Unit = {
  }

  def pushToKafka(scanEntry: String): Boolean = {

    //Enrich a prop file with necessary values from application.properties
    val kafkaSetProp = getEnrichedProp()

    //Get topic, key and value(payload) for pushing to Kafka
    val topic = kafkaSetProp.get("prod.topic-name").asInstanceOf[String]
    val kaf_key = "Scan_ScreenView"

    //Create a Kafka Producer and ProducerRecord
    val producer = new KafkaProducer[String, String](kafkaSetProp)
    val producerRecord = new ProducerRecord[String, String](topic,kaf_key,scanEntry)
    var getMetafromKafka: Any = null

    //Try pushing to Kafka. Adding get() is a callback which will ensure record is sent to buffer and committed also.
    try {
      val metaProducer: RecordMetadata = producer.send(producerRecord).get()
      getMetafromKafka = metaProducer
      println(s"Record pushed to the Kafka Topic: $topic")
      println(s"Sent Record[Key:${producerRecord.key()}, Value:${producerRecord.value()}")
      println(s"Received in Metadata: ${metaProducer.offset()}; ${metaProducer.partition()}")
    }
    catch {
      case ex: Exception => ex.printStackTrace()
        //Return false in case of failure
        return false
    }
    finally {
      producer.close()
    }
    //Return true in case of successful push
    true
  }


  def getEnrichedProp() = {

    //Keep all properties config at /src/main/resources
    val appUrl = getClass.getResource("producer.properties")
    val tempProp = new Properties()

    if (appUrl != null) {
      val fileSource = Source.fromURL(appUrl)
      tempProp.load(fileSource.bufferedReader())
    }

    tempProp
  }

}