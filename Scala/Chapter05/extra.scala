// Databricks notebook source
// MAGIC %md
// MAGIC ***Pros y cons de utilizar UDFs***

// COMMAND ----------

El beneficio es que se pueden usar dentro de Spark SQL. Los demás pueden utilizar estas funciones sin tener conocimientos de qué están haciendo realmente
La contra es que penaliza mucho el rendimiento, sobre todo con Python ya que tiene que serializar cada fila del dataframe, enviar a Python Runtime y devolver a la JVM

// COMMAND ----------

// MAGIC %md
// MAGIC ***Instalar MySQL, descargar driver y cargar datos de BBDD de empleados***  
// MAGIC Hecho en Local  
// MAGIC * Hay que instalar Mysql
// MAGIC * Descargar jar jdbc
// MAGIC * Iniciar la spark-shell --jars "nombre_del_jar.jar"
// MAGIC * val employeesDF = spark .read .format("jdbc") .option("url", "jdbc:mysql://localhost:3306/employees") .option("driver", "com.mysql.jdbc.Driver") .option("dbtable", "employees") .option("user", "root") .option("password", "root") .load()
// MAGIC * val departmentsDF = spark .read .format("jdbc") .option("url", "jdbc:mysql://localhost:3306/employees") .option("driver", "com.mysql.jdbc.Driver") .option("dbtable", "departments") .option("user", "root") .option("password", "root") .load()
// MAGIC * val dept_empDF = spark .read .format("jdbc") .option("url", "jdbc:mysql://localhost:3306/employees") .option("driver", "com.mysql.jdbc.Driver") .option("dbtable", "dept_emp") .option("user", "root") .option("password", "root") .load()
// MAGIC * val df1 = employeesDF.join(dept_empDF,employeesDF("emp_no")===dept_empDF("emp_no"),"inner")
// MAGIC * val df2 = df1.join(departmentsDF,df1("dept_no")===departmentsDF("dept_no"),"inner")

// COMMAND ----------

// MAGIC %md
// MAGIC ***Diferencia entre Rank y dense_rank (operaciones de ventana)***

// COMMAND ----------

En rank cuando hay empate salta al siguiente número, en cambio en dense_rank sigue la secuencia

Ventana particionada por department y ordenada por salario

rank()
+-------------+----------+------+----+
|employee_name|department|salary|rank|
+-------------+----------+------+----+
|        James|     Sales|  3000|   1|
|        James|     Sales|  3000|   1|
|       Robert|     Sales|  4100|   3|
|         Saif|     Sales|  4100|   3|
|      Michael|     Sales|  4600|   5|
|        Maria|   Finance|  3000|   1|
|        Scott|   Finance|  3300|   2|
|          Jen|   Finance|  3900|   3|
|        Kumar| Marketing|  2000|   1|
|         Jeff| Marketing|  3000|   2|
+-------------+----------+------+----+

dense_rank()
+-------------+----------+------+----------+
|employee_name|department|salary|dense_rank|
+-------------+----------+------+----------+
|        James|     Sales|  3000|         1|
|        James|     Sales|  3000|         1|
|       Robert|     Sales|  4100|         2|
|         Saif|     Sales|  4100|         2|
|      Michael|     Sales|  4600|         3|
|        Maria|   Finance|  3000|         1|
|        Scott|   Finance|  3300|         2|
|          Jen|   Finance|  3900|         3|
|        Kumar| Marketing|  2000|         1|
|         Jeff| Marketing|  3000|         2|
+-------------+----------+------+----------+