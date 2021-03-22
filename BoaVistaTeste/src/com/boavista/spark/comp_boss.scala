package com.boavista.spark

import org.apache.spark.sql.SparkSession


object comp_boss {
  
  def main(args: Array[String]) {
  
    //Iniciando sessão Spark
  val spark = SparkSession.builder().getOrCreate()
  
  //Persistindo o bucket para a leitura no Bigquery
  val bucket = "bucket-boavista-iago-comp-boss"
  spark.conf.set("temporaryGcsBucket", bucket)
   
 //Lendo o arquivo no diretorio
  val df_price_quote = spark.read.option("header","true")
      .csv("gs://plenary-atrium-308117/comp_boss.csv")
      
   //Save Table no Bigquer   
  df_price_quote.write.format("com.google.cloud.spark.bigquery")
  .option("table","boavista.comp_boss")
  .save()
  
  }
  
}