// Databricks notebook source
// MAGIC %md
// MAGIC ***GlobalTempView vs TempView***

// COMMAND ----------

// MAGIC %md
// MAGIC La GlobalTempView estará disponible en cualquier sesión, en cambio la TempView sólo estará disponible en la sesión con la que la hayamos creado

// COMMAND ----------

// MAGIC %md
// MAGIC ***Leer los AVRO, Parquet, JSON y CSV escritos en el cap3***

// COMMAND ----------

//JSON to DataFrame
val jsonFile = "/databricks-datasets/learning-spark-v2/blogs.json"
val jsondf = spark.read.format("json")
  .load(jsonFile)
display(jsondf)

// COMMAND ----------

// MAGIC %sql
// MAGIC --JSON to SQL Table
// MAGIC CREATE OR REPLACE TEMPORARY VIEW blogs_tbl
// MAGIC  USING json
// MAGIC  OPTIONS (
// MAGIC    path "/databricks-datasets/learning-spark-v2/blogs.json"
// MAGIC  )

// COMMAND ----------

 display(spark.sql("select * from blogs_tbl"))

// COMMAND ----------

//CSV to DataFrame
val csvFile = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
val csvdf = spark.read.format("csv")
  .option("header","true")
  .option("inferSchema","true")
  .load(csvFile)
display(csvdf)

// COMMAND ----------

// MAGIC %sql
// MAGIC --CSV to SQL Table
// MAGIC CREATE OR REPLACE TEMPORARY VIEW fire_calls_tbl
// MAGIC  USING csv
// MAGIC  OPTIONS (
// MAGIC    path "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv",
// MAGIC    header "true",
// MAGIC    inferSchema "true"
// MAGIC  )
// MAGIC  display(spark.sql("select * from fire_calls_tbl"))