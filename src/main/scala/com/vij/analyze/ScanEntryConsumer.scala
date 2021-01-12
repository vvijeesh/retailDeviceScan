package com.vij.analyze

import com.vij.analyze.dataIntegrity._
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.sql.functions.{column, explode, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.JavaConverters._
import java.util.{Calendar, Properties}
import scala.io.Source

class ScanEntryConsumer(args: Array[String]) {

  //Default Constructor
  def ScanEntryConsumer(): Unit = {
  }

  def main(args: Array[String]) = {

    //Enrich a prop file with necessary values from application.properties
    val props = getConsumerEnrichedProp()

    val spark = SparkSession
      .builder()
      .appName("RetailScanData")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    val spconf = new SparkConf().setAppName("ScanData_Consumer")
    val spcontext = new SparkContext(spconf)
    val topic = props.get("topic-name").asInstanceOf[String]
    val kafkaTopics = topic.split(",").toSet


    //Create a streaming context of 30m interval
    val ssc = new StreamingContext(spcontext, Seconds(1800))
    val kafkaParams = Map[String, String](
      "broker-list" -> "localhost:9092",
      "group-id" -> "retail_scan",
      "auto-offset-reset" -> "earliest",
      "enable.auto.commit" -> "false",
      "key-deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value-deserializer" -> "org.springframework.kafka.support.serializer.JsonDeserializer",
      "properties.spring.json.trusted.packages" -> "*"
    )

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaParams))

    try {
      dStream.foreachRDD { rawdata_rdd =>
        //Func() to prepare final target data
        prepareScanDataTables(rawdata_rdd, spark)
        //Prepare and commit offsets
        val offsetRanges = rawdata_rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
    }

    ssc.start()
    //Await for stop or SIGTERM from user
    ssc.awaitTermination()
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

  def prepareScanDataTables(given_rdd: RDD[ConsumerRecord[String, String]], spark: SparkSession) = {


    given_rdd.foreach { c =>
      println("Processing data for KEY: " + c.key())

      val newSchema = DataType.fromJson(c.value).asInstanceOf[StructType]
      val finaldf = spark.read.option("multiLine", true).schema(newSchema).json(c.value())

      //=======================
      // Prepare Product Dataframe
      //=======================

      import spark.implicits._
      var df1 = finaldf.select("properties.*")
      var df2: DataFrame = df1.select("storeId", "shoppingCartEvents").toDF()
      var df3 = df2.select(col("storeId"), explode(col("shoppingCartEvents")).alias("shopCart"))
      var df4 = df3.select("storeId", "shopCart.*")

      /*

      df4.printSchema
      root
        |-- storeId: string (nullable = true)
        |-- added: boolean (nullable = true)
        |-- ean: string (nullable = true)
        |-- productName: string (nullable = true)
        |-- timestamp: long (nullable = true)

       Sample Data

        +-------+-----+-------------+------------------+-------------+
        |storeId|added|ean          |productName       |timestamp    |
        +-------+-----+-------------+------------------+-------------+
        |002513 |true |7350025784359|Pappersbärkasse   |1605855579775|
        |002513 |true |7350025784359|Pappersbärkasse   |1605855580474|
        |002513 |true |7350025784359|Pappersbärkasse   |1605855581121|
       */

      spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

      //Validate and clean/load the product table - productScanDF
      var productScanDF: DataFrame = getValidatedProductDF(df4)

      //Store the product table - productScanDF
      saveProductTable(productScanDF)

      //=======================
      //Prepare trans dataframe
      //=======================

      var dd1 = df1.select("finalReceipt.entities")
      var dd2 = dd1.select(explode(col("entities")).alias("shopEntity"))
      var dd3 = dd2.select("shopEntity.*")

      /*
      Print dd3

      +------+-------------+--------+
      |amount_Item  |  ean_Product_Id  |  product_Quantity |
      +------+-------------+--------+
      |20.00 |7350025784359|4       |
      |99.80 |7331210165382|2       |
      |17.00 |2092401117003|1       |

      */

      //Validate and clean/load trans table DF - productTransDF
      val productTransactionDF: DataFrame = getValidatedTransactionDF(dd3)

      //Store the product Transaction table - productTransactionDF
      saveTransactionTable(productTransactionDF)

      //Aggregate Check all entries based on trans and product table - ean : product_ID
      var summaryTable: DataFrame = productScanDF.join(productTransactionDF, productScanDF("ean_Product_Id") === productTransactionDF("ean_Product_Id"), "outer")

      // Perform summary based aggregation
      prepareSummaryTables(summaryTable)

      /*
      +-------+-----+-------------+------------------+-------------+------+-------------+--------+
      |storeId|added|ean          |productName       |timestamp    |amount|ean          |quantity|
      +-------+-----+-------------+------------------+-------------+------+-------------+--------+
      |002513 |true |7318690163121|Flytande tvätt    |1605857602357|24.95 |7318690163121|1       |
      |002513 |true |7330797088855|Cognacsmedw tsk   |1605856952290|19.90 |7330797088855|1       |
      |002513 |true |7331210165382|LED Miniglob 250lm|1605856082299|99.80 |7331210165382|2       |
      |002513 |true |7331210165382|LED Miniglob 250lm|1605856083125|99.80 |7331210165382|2       |
      |002513 |true |5701092113177|Pure Refill Cherry|1605857598141|49.95 |5701092113177|1       |
       */

    }
  }

  def prepareSummaryTables(givenDF: DataFrame) = {

    /* === Create Summary Table:

    Total Transaction Value by Store
    Total Transaction Value by Product
    Total Transaction Value by Hour of the Day

    */

    //Store wise Summary
    givenDF.groupBy("store_Id").sum("amount_Item").show(false)

    //Product wise Summary
    givenDF.groupBy("ean_Product_Id").sum("amount_Item").show(false)

    //Time wise summary (Hour wise)
    givenDF.withColumn("hour",hour(col("timestamp_at_Scan"))).groupBy("hour").sum("amount_Item").show(false)
  }
}
