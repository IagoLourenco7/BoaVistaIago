package com.boavista.spark

import org.apache.spark.sql.SparkSession


object bill_of_materials {
  
  def main(args: Array[String]) {
  
    //Iniciando sessão Spark
  val spark = SparkSession.builder().getOrCreate()
  
  //Persistindo o bucket para a leitura no Bigquery
  val bucket = "bucket-boavista-iago-bill-of-materials"
  spark.conf.set("temporaryGcsBucket", bucket)
    
  //Lendo o arquivo no diretorio
  val df_price_quote = spark.read.option("header","true")
      .csv("gs://plenary-atrium-308117/bill_of_materials.csv")
      
   //Save Table no Bigquery   
  df_price_quote.write.format("com.google.cloud.spark.bigquery")
  .option("table","boavista.bill_of_materials")
  .save()
  
  
  //Caso queira executar o processo de save da tabela 'DESPITVOTADA', 
  //comentar o save table acima e executar o trecho abaixo
      
//   val df_unpivot = df_price_quote.select("tube_assembly_id","component_id_1","quantity_1")
//                    .unionAll(df_price_quote.select("tube_assembly_id","component_id_2","quantity_2"))
//                    .unionAll(df_price_quote.select("tube_assembly_id","component_id_3","quantity_3"))
//                    .unionAll(df_price_quote.select("tube_assembly_id","component_id_4","quantity_4"))
//                    .unionAll(df_price_quote.select("tube_assembly_id","component_id_5","quantity_5"))
//                    .unionAll(df_price_quote.select("tube_assembly_id","component_id_6","quantity_6"))
//                    .unionAll(df_price_quote.select("tube_assembly_id","component_id_7","quantity_7"))
//                    .unionAll(df_price_quote.select("tube_assembly_id","component_id_8","quantity_8"))
//                    .toDF("tube_assembly_id","component_id","quantity")
//                    
//  df_unpivot.write.format("com.google.cloud.spark.bigquery")
//  .option("table","boavista.bill_of_materials_unpivot")
//  .save()
      
  }
  
}