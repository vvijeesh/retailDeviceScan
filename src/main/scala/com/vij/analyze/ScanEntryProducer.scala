package com.vij.analyze

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties
import java.io.{FileInputStream, FileNotFoundException}

class ScanEntryProducer() {

  //Default Constructor
  def ScanEntryProducer(): Unit = {
  }

  def pushToKafka(scanEntry: ProducerRecord[String, String], kafkaProp: String) = {

    //Enrich a prop file with necessary values for Kafka Producer
    val kafkaSetProp = getEnrichedProp(kafkaProp)

    val topic = kafkaSetProp.get("prod.topic-name")

    val producer = new KafkaProducer[String, String](kafkaSetProp)
    var getMetafromKafka: Any = null

    try {
      val metaProducer: RecordMetadata = producer.send(scanEntry).get()
      getMetafromKafka = metaProducer
    }
    catch {
      case e: Exception => e.printStackTrace();
    }
    finally {
      producer.close()
    }
    getMetafromKafka
  }


  def readPropFile(fileName: String): Properties = {
    val prop = new Properties()
    try {
      val filestream = new FileInputStream(fileName)
      prop.load(filestream)
    }
    catch {
      case ex: FileNotFoundException => println(s"File $fileName is not found"); ex.printStackTrace()
    }
    //Return prop file
    prop
  }

  def getEnrichedProp(tempKafka: String) = {
    val propFileName = readPropFile(tempKafka)
    val tempProp = new Properties()

    tempProp.put("prod.bootstrap-servers", propFileName.get("prod.bootstrap-servers"))
    tempProp.put("prod.key-serializer", propFileName.get("prod.key-serializer"))
    tempProp.put("prod.value-serializer", propFileName.get("prod.value-serializer"))
    tempProp.put("prod.acks", propFileName.get("prod.acks"))

    tempProp
  }

}