package com.boavista.spark

import org.apache.spark.sql.SparkSession


object bill_of_materials {
  
  def main(args: Array[String]) {
  
  val spark = SparkSession.builder().getOrCreate()
  
  val bucket = "bucket-boavista-iago-bill-of-materials"
  spark.conf.set("temporaryGcsBucket", bucket)
    
  val df_price_quote = spark.read.option("header","true")
      .csv("gs://plenary-atrium-308117/bill_of_materials.csv")
      
      
  df_price_quote.write.format("com.google.cloud.spark.bigquery")
  .option("table","boavista.bill_of_materials")
  .save()
  
  }
  
}