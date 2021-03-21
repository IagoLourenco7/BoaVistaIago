package com.boavista.spark


import org.apache.spark.sql.SparkSession

object pricequote {
  
  
def main(args: Array[String]) {
  
val spark = SparkSession.builder().getOrCreate()
    

val bucket = "bucket-boavista-iago-price-quote"
spark.conf.set("temporaryGcsBucket", bucket)
    
  val df_price_quote = spark.read.option("header","true")
      .csv("gs://plenary-atrium-308117/price_quote.csv")
      
  df_price_quote.write.format("com.google.cloud.spark.bigquery")
  .option("table","boavista.price_quote")
  .save()
  
  
  
  }
  
}


