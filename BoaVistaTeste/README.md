Baixar o código e criar o .jar.

Foram criados 3 jobs no Dataproc para a execução das ingestões dos arquivos do BigQuery.

Script de criação:
gcloud beta dataproc clusters create boavista_teste_engDados\
 --region southamerica-east1 \
 --zone southamerica-east1-b \
 --single-node\
 --master-machine-type\
 n1-standard-4\
 --master-boot-disk-size 500 \
 --image-version 2.0-debian10 \
 --project plenary-atrium-308117 \
 --initialization-actions gs://goog-dataproc-initialization-actions-southamerica-east1/connectors/connectors.sh \
 --metadata gcs-connector-version=2.2.0 \
 --metadata bigquery-connector-version=1.2.0 \
 --metadata spark-bigquery-connector-version=0.19.1
 
 
 Necessário criar 4 buckets
 
 gsutil mb gs://plenary-atrium-308117 -- > (Armazenar os 3 arquivos e o .jar nesse bucket)
 
  gsutil mb gs://bucket-boavista-iago-bill-of-materials
  gsutil mb gs://bucket-boavista-iago-comp-boss
  gsutil mb gs://bucket-boavista-iago-price-quote
  
 Os três últimos serão os buckets temporários paara o bigquery
 
 Obs: Como os Buckets tem nome único, recomendo trocar o nome para o desejado e alterar o 
 código fonte para o mesmo nome
 
 Após a criação dos buckets e do cluster dataproc, criar os jobs nas opçõe:
 Menu de Navegação -> DataProc -> Jobs
 
 Clique em enviar Job, preencha o nome, selecione o cluster(boavista_teste_engDados)
  a regiao do cluster (southamerica-east1), o tipo de Job selecione Spark
 No Campo Classe Principal ou jar preencha: com.boavista.spark.bill_of_materials, no campo arquivos jar, 
 aponte para o bucket onde o .jar foi salvo
 
 Repita o passo anterior mais duas vezes alterando apenas a 
 Classe Principal: com.boavista.spark.pricequote e com.boavista.spark.comp_boss

Após a execução do job, os arquivos estarão no bigquery
boavista.price_quote
boavista.bill_of_materials
boavista.comp_boss

Caso queira explorar os dados, tube_assembly_id está presente bill_of_materials
e a chave de comp_boss esta presente em bill_of_materials, porém, antes de fazer o relacionamento, 
deve ser feito
um "despivoteamento", já que o campo component_id da tabela comp_boss está pivoteada em colunas na tabela
bill_of_materials.

Caso queria um relatório, faça a consulta desejada e selecione a opção Explorar Dados -> Explorar Dados com
Data Estudio

Segue um gráfico feito com a tabela price_quote, onde foi executada a query:

select tube_assembly_id, sum(cast(quantity as numeric)) as soma
from boavista.price_quote 
group by  tube_assembly_id, quote_date;

Criação da view despivotada caso não seja executada o segundo processo do script bill_of_materials

CREATE VIEW boavista.vw_bill_of_materials as
select tube_assembly_id, 
       component_id_1 as component_id, 
       quantity_1 as quantity
from boavista.bill_of_materials
union all
select tube_assembly_id, 
       component_id_2 as component_id, 
       quantity_2 as quantity
from boavista.bill_of_materials
union all 
select tube_assembly_id, 
       component_id_3 as component_id, 
       quantity_3 as quantity
from boavista.bill_of_materials
union all 
select tube_assembly_id, 
       component_id_4 as component_id, 
       quantity_4 as quantity
from boavista.bill_of_materials
union all
select tube_assembly_id, 
       component_id_5 as component_id, 
       quantity_5 as quantity
from boavista.bill_of_materials
union all 
select tube_assembly_id, 
       component_id_6 as component_id, 
       quantity_6 as quantity
from boavista.bill_of_materials
union all 
select tube_assembly_id, 
       component_id_7 as component_id, 
       quantity_7 as quantity
from boavista.bill_of_materials
union all 
select tube_assembly_id, 
       component_id_8 as component_id, 
       quantity_8 as quantity
from boavista.bill_of_materials;


Retornando a soma de tubos montados por tipo conforme o id
Segue link de amostra:


https://datastudio.google.com/reporting/2faa8d9c-e92c-4424-b459-644d3cb94574

 
