package com.vij.analyze

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies, KafkaUtils}

import scala.collection.JavaConverters._
import java.util.{Calendar, Properties}
import scala.io.Source

class ScanEntryConsumer(args: Array[String]) {

  //Default Constructor
  def ScanEntryConsumer(): Unit = {
  }

  def main (args: Array[String]) = {

    //Enrich a prop file with necessary values from application.properties
    val props = getConsumerEnrichedProp()

    val checkDir = "file:::F:\\Cockroach\\retailDeviceScan\\src\\main\\resources"

    val spconf = new SparkConf().setAppName("ScanData_Consumer")
    val spcontext = new SparkContext(spconf)

    //Create a streaming context of 30m interval
    val ssc = new StreamingContext(spcontext,Seconds(1800))

    val dStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.
    try {

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }

  def getConsumerEnrichedProp() = {

    //Keep all properties config at /src/main/resources
    val appUrl = getClass.getResource("consumer.properties")
    val tempProp = new Properties()

    if (appUrl != null) {
      val fileSource = Source.fromURL(appUrl)
      tempProp.load(fileSource.bufferedReader())
    }

    tempProp
  }

}
