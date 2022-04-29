# Databricks notebook source
from pyspark.sql import SparkSession
  
spark = SparkSession.builder.appName('Read CSV File into DataFrame').getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ***Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto.***

# COMMAND ----------

mnmFile = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"
mnmDF = spark.read.csv(mnmFile,sep=",",inferSchema=True,header=True)
mnmDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ***Cuando se define un schema al definir un campo por ejemplo StructField('Delay', FloatType(), True) ¿quésignifica el último parámetro Boolean?***   
# MAGIC 
# MAGIC SIGNIFICA SI PUEDE SER NULL O NO

# COMMAND ----------

# MAGIC %md
# MAGIC ***DataSet vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?***  
# MAGIC 
# MAGIC Cuando actuamos en el DataSet, tratamos por objetos, en el DataFrame por columnas
# MAGIC En el DataSet tendremos que crear una clase para definir la estructura(tipado obligatorio)
# MAGIC En el DataFrame definimos un esquema para definir la estructura

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ***Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y guardar los datos en los formatos:***
# MAGIC * JSON
# MAGIC * CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
# MAGIC * AVRO

# COMMAND ----------

#Utilizaremos el DF de M&M que anteriormente hemos usado

mnmDF.write.format("JSON").save("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com/mnmDF_JSON2") #JSON

mnmDF.write.format("CSV").save("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com/mnmDF_CSV2") #CSV

mnmDF.write.format("AVRO").save("dbfs:/FileStore/shared_uploads/alejandro.ibanez@bosonit.com/mnmDF_AVRO2") #AVRO



# COMMAND ----------

# MAGIC %md
# MAGIC **Eliminar carpetas en databricks**  
# MAGIC dbutils.fs.rm(*ruta*, true)