package com.vij.analyze

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, from_unixtime, udf, when}

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.{Calendar, Date}

object dataIntegrity {

  //Sample func to validate Age
  def validateDate(givenDate: String): Option[Date] = {

    var converted_date = ""
    try {
      //TimeinMillis 1610351511784
      var timeinMilli: Long = new Date().getTime
      if (givenDate.toLong < timeinMilli) {
        converted_date = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(givenDate.toLong)
        Some[String]
      }
      else
        None
    } catch {
      case e: Exception => println("Exception");
        e.getMessage();
        e.printStackTrace()
        None
    }
  }

  //Validation Functions
  def boolCheck(value: String): String = {
    if (value == "true" || value == "false") {
      value
    } else null
  }

  //Register UDF for BoolCheck
  val udfBoolCheck = udf(boolCheck _)


  //Validation for Product Scan DF
  def getValidatedProductDF(givenDF: DataFrame) = {

    //Get List of Columns in Seq to delete
    val dropProductColumns = givenDF.columns.toSeq

    givenDF
      .withColumn("store_Id", (when(col("storeId").cast("int").isNotNull, col("storeId")).otherwise(null)).cast("int"))
      .withColumn("product_Scanned", udfBoolCheck(col("productScanned")).cast("Boolean"))
      .withColumn("ean_Product_Id", when(col("productId").cast("long").isNotNull, col("productId")).otherwise(null).cast("long"))
      .withColumn("product_Name", when(col("productName").cast("String").isNotNull, col("productName")).otherwise(null).cast("String"))
      .withColumn("timestamp_at_Scan", from_unixtime(col("timestamp") / 1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
      .drop(dropProductColumns: _*)
  }


  //Validation for Product Transaction DF
  def getValidatedTransactionDF(givenDF: DataFrame) = {

    //Get List of Columns in Seq to delete
    val dropTransColumns = givenDF.columns.toSeq

    givenDF
      .withColumn("amount_Item",(when(col("amount").cast("double").isNotNull,col("amount")).otherwise(null)).cast("double"))
      .withColumn("ean_Product_Id",(when(col("ean").cast("long").isNotNull,col("ean")).otherwise(null)).cast("long"))
      .withColumn("product_Quantity",(when(col("quantity").cast("int").isNotNull,col("quantity")).otherwise(null)).cast("int"))
      .drop(dropTransColumns:_*)

  }

  def saveProductTable(givenDF: DataFrame) = {
    givenDF.toString()
    try {
      givenDF.write.mode(SaveMode.Append).saveAsTable("productScanDF.parquet")
    }
    catch {
      case e: Exception => println("Exception in saveProductTable"); e.getMessage; e.printStackTrace()
    }
    println("Records written to 'productScanDF' table successfully")
  }

  def saveTransactionTable(givenDF: DataFrame) = {
    givenDF.toString()
    try {
      givenDF.write.mode(SaveMode.Append).saveAsTable("productTransactionDF.parquet")
    }
    catch {
      case e: Exception => println("Exception in saveProductTable"); e.getMessage; e.printStackTrace()
    }
    println("Records written to 'productTransactionDF' table successfully")
  }

}
