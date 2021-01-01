package com.vij.analyze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

class DataConsumer(args: Array[String]) {

  //Default Constructor
  def DataConsumer() {
  }

  val sourceFile = args(0)
  val jsonType = args(1)

  val spark = SparkSession
  .builder()
  .appName("WeatherForecast")
  .master("yarn")
  .enableHiveSupport()
  .getOrCreate()

  val rdd = spark.sparkContext.textFile(sourceFile)
  val newSchema=DataType.fromJson(jsonType).asInstanceOf[StructType]
  val finaldf=spark.read.option("multiLine", true).schema(newSchema).json(jsonType)

}
