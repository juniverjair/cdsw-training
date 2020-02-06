
# ## 




# ## Leer y escribir



# *  Spark SQL `DataFrameReader` y `DataFrameWriter` soporta los siguientes fuentes:
#   * text
#   * delimited text
#   * JSON (JavaScript Object Notation)
#   * Apache Parquet
#   * Apache ORC
#   * Apache Hive
#   * JDBC connection

# * Spark SQL se integra con el framework de Pandas.

# * Fuentes adicionales soportados a través de terceros [third-party
# packages](https://spark-packages.org/).


# ## Setup


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("read-cvillarroel").getOrCreate()

# Crear un directorio HDFS  para guardar los datos:

# Remover un directorio existente
!hdfs dfs -rm -r -skipTrash data  

!hdfs dfs -mkdir data


# ## Trabajar con archivos delimitados

# Usar
# [csv](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.csv)
# Metodo de 
# [DataFrameReader](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)
# para leer archivos deliminados:
riders = spark \
  .read \
  .csv("/duocar/raw/riders/", sep=",", header=True, inferSchema=True) \

#  `csv` sintaxis mas general
# syntax:
riders = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .option("inferSchema", True) \
  .load("/duocar/raw/riders/")

# **Nota:** si usas  `header` igual a`True`, 
# aspark asume que cada archivo en el directorio tiene una fila de cabecera 


riders.printSchema()

# Especificar los tipos de datos
# [types](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)
# por un esquema:
from pyspark.sql.types import *

# Then specify the schema as a `StructType` instance: 
schema = StructType([
    StructField("id", StringType(), True),
    StructField("birth_date", DateType(), True),
    StructField("join_date", DateType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("student", IntegerType(), True),
    StructField("home_block", StringType(), True),
    StructField("home_lat", DoubleType(), True),
    StructField("home_lon", DoubleType(), True),
    StructField("work_lat", DoubleType(), True),
    StructField("work_lon", DoubleType(), True)
])

# pasar el esquema al lector de datos`:
riders2 = spark \
  .read \
  .format("csv") \
  .option("sep", ",") \
  .option("header", True) \
  .schema(schema) \
  .load("/duocar/raw/riders/")

# **Note:** hay que incluir la opcion header caso contrario leera 
# la cabecera del archivo con un registro mas


# confirmar el esquema:
riders2.printSchema()

# usar el metodo  `csv` 
# [DataFrameWriter](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)
# para estribir del dataframe a un archivo delimitado por tabuladores:
riders2.write.csv("data/riders_tsv/", sep="\t")
!hdfs dfs -ls data/riders_tsv

# **Note:** el archivo tiene una extension  `csv` aun con el separador tab


# usar el parametro `mode` para sobreescribir un archivo existente y 
# el argumento `compression` especifica un codificador de compresion
riders2.write.csv("data/riders_tsv_compressed/", sep="\t", mode="overwrite", compression="bzip2")
!hdfs dfs -ls data/riders_tsv_compressed

# ver referencias de compresion [Data
# Compression](https://www.cloudera.com/documentation/enterprise/latest/topics/introduction_compression.html)



# ## trabajando con archivos de texto

# Usar 
# [text](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader.text)
# el metodo de la clase  `DataFrameReader` para leer un archivo no estructurado:
weblogs = spark.read.text("/duocar/earcloud/apache_logs/")
weblogs.printSchema()
weblogs.head(5)

# **Nota:** el filesystem por defecto para CDH (y por consecuencia CDSW) es HDFS.
# la sentencia indicada anteriormente es un shortcutt hacia esta sentencia 
#```python
#weblogs = spark.read.text("hdfs:///duocar/earcloud/apache_logs/")
#```
# de forma general tiene estos parametros adicionales de host y puerto
#```python
#weblogs = spark.read.text("hdfs:/<host:port>//duocar/earcloud/apache_logs")
#```
# donde  `<host:port>` onde el host y el puerto del nodo HDFS 

# Parsear los datos no estructurados:
from pyspark.sql.functions import regexp_extract
requests = weblogs.select(regexp_extract("value", "^.*\"(GET.*?)\".*$", 1).alias("request")) 
requests.head(5)

# Use the
# [text](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter.text)
# metodo para escribir un dato no estructurado en texto de la clase  `DataFrameWriter` :
requests.write.text("data/requests_txt/")
!hdfs dfs -ls data/requests_txt


# ## Trabajando con 

# [Parquet](https://parquet.apache.org/) es un formato de almacenamiento columnar 
# para   Hadoop.  Es el default para SparkSQL .  Usar
# el metodo  `parquet` de la clase `DataFrameWriter` para escribir en un parquet:
riders2.write.parquet("data/riders_parquet/")
!hdfs dfs -ls data/riders_parquet

# **Nota:** el mensaje de SLF4J .

# usar el metodo red `parquet` de la clase  `DataFrameReader` 
# Parquet file:
spark.read.parquet("data/riders_parquet/").printSchema()

# **Nota:** Spark usa el esquema almacenado de la data.

# Formato Ocr

riders2.write.orc("data/riders_orc/")
!hdfs dfs -ls data/riders_orc
spark.read.orc("data/riders_orc/").printSchema()

# ## Trabajar con tablas Hive

# Use the `sql` method of the `SparkSession` class to run Hive queries:

spark.sql("USE default")
spark.sql("SHOW TABLES").show()
spark.sql("DESCRIBE riders").show()
spark.sql("SELECT * FROM riders LIMIT 10").show()

# usar el metodo  `table`  `DataFrameReader` para leer la tabla Hive:
riders_table = spark.read.table("riders")
riders_table.printSchema()
riders_table.show(5)

# Usar el método `saveAsTable`  `DataFrameWriter` para escribir a una tabla  Hive

import uuid
table_name = "riders_" + str(uuid.uuid4().hex)  # Create unique table name.
riders.write.saveAsTable(table_name)


# Se puede manipular esta talba con Hive, impala o por spark sql
spark.sql("DESCRIBE %s" % table_name).show()



# ## trabajar con pandas


import pandas as pd

# subir el archivo en la ruta local 
demographics_pdf = pd.read_csv("data/demographics.txt", sep="\t")

# Acceder a los tipos de la estructura `dtypes` :
demographics_pdf.dtypes

# Usar pandas para ver los datos
demographics_pdf.head()

# Usar  el metodo  `createDataFrame` de la clase `SparkSession` para crear un dataframe Spark
demographics = spark.createDataFrame(demographics_pdf)
demographics.printSchema()
demographics.show(5)

# usar  `toPandas` para volver a verlo como pandas:
riders_pdf = riders.toPandas()
riders_pdf.dtypes
riders_pdf.head()




# ## References

# [Spark Python API - pyspark.sql.DataFrameReader
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader)

# [Spark Python API - pyspark.sql.DataFrameWriter
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter)


# ## Cleanup

# Drop the Hive table:
spark.sql("DROP TABLE IF EXISTS %s" % table_name)

# Stop the SparkSession:
spark.stop()
