package com.boavista.spark

import org.apache.spark.sql.SparkSession


object comp_boss {
  
  def main(args: Array[String]) {
  
  val spark = SparkSession.builder().getOrCreate()
  
  val bucket = "bucket-boavista-iago-comp-boss"
  spark.conf.set("temporaryGcsBucket", bucket)
    
  val df_price_quote = spark.read.option("header","true")
      .csv("gs://plenary-atrium-308117/comp_boss.csv")
      
      
  df_price_quote.write.format("com.google.cloud.spark.bigquery")
  .option("table","boavista.comp_boss")
  .save()
  
  }
  
}