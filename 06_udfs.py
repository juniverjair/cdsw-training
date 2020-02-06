
# ## Overview




# ## User-Defined Functions

# * Es relativamente sencillo aplicar lo que se conoce como user-defined function (UDF)
#   Aplicar la funcion UDF 

# * para usarlo python y los paquetes requeridos deben de ser instalados en los 
# equipos workers 
#   * Es posible distribuir el paquete via spark 


# * UDFs son ineficientes debido a :
#   * El proceso de Python debe iniciarse junto con cada ejecutor
#   * Los datos deben convertirse entre tipos Java y Python
#   * Los datos deben transferirse entre los procesos de Java y Python.

# * Ver opciones para mejorar un UDF:
#   * Use the [Apache Arrow](https://arrow.apache.org/) platform 
#   * Usar una funcion vectorizada (see the `pandas_udf` function)
#   * Reescribir la funcion en Scala o Java


# ## Setup


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("udfs").getOrCreate()

rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)

# Castear `date_time` a timestamp:
from pyspark.sql.functions import col
rides_clean = rides.withColumn("date_time", col("date_time").cast("timestamp"))


# ## Ejemplo 1

# Usar la funcion definida:
import datetime
def hour_of_day(timestamp):
  return timestamp.hour

# **Nota:** El tipo en Spark `TimestampType` es en Python el tipo `datetime.datetime`.

# Probar la funcion
dt = datetime.datetime(2017, 7, 21, 5, 51, 10)
hour_of_day(dt)

# Registrarlo como UDF:
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
hour_of_day_udf = udf(hour_of_day, returnType=IntegerType())

# **Note:** We must explicitly specify the return type otherwise it defaults
# to `StringType`.

# aplicar el  UDF:
rides_clean \
  .select("date_time", hour_of_day_udf("date_time")) \
  .show(5, truncate=False)

# Use the UDF to compute the number of rides by hour of day:
rides_clean \
  .select(hour_of_day_udf("date_time").alias("hour_of_day")) \
  .groupBy("hour_of_day") \
  .count() \
  .orderBy("hour_of_day") \
  .show(25)


# ## Ejemplo 2: Distancia usando la formula haversine

# The [great-circle
# distance](https://en.wikipedia.org/wiki/Great-circle_distance) es la 
# distancia mas corta entre dos puntos en la superficie de una esfera.  In this
# ejemplo para crear una funcion definida [haversine
# approximation](https://en.wikipedia.org/wiki/Haversine_formula) entre el origen y el destino.

# Definir la funcion haversine
# [rosettacode.org](http://rosettacode.org/wiki/Haversine_formula#Python)):
from math import radians, sin, cos, sqrt, asin
def haversine(lat1, lon1, lat2, lon2):
  """
  Return the haversine approximation to the great-circle distance between two
  points (in meters).
  """
  R = 6372.8 # Earth radius in kilometers
 
  dLat = radians(lat2 - lat1)
  dLon = radians(lon2 - lon1)

  lat1 = radians(lat1)
  lat2 = radians(lat2)
 
  a = sin(dLat / 2.0)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2.0)**2
  c = 2.0 * asin(sqrt(a))
 
  return R * c * 1000.0


# probar:
haversine(36.12, -86.67, 33.94, -118.40)  
# = 2887259.9506071107:

# Registrar la funcion como UDF:
from pyspark.sql.types import DoubleType
haversine_udf = udf(haversine, returnType=DoubleType())

# Aplicar la funcion UDF:
distances = rides \
  .withColumn("haversine_approximation", haversine_udf("origin_lat", "origin_lon", "dest_lat", "dest_lon")) \
  .select("distance", "haversine_approximation")
distances.show(5)

# Esperamos que sea menor a la distancia registrada:
distances \
  .select((col("haversine_approximation") > col("distance")).alias("haversine > distance")) \
  .groupBy("haversine > distance") \
  .count() \
  .show()

# Los valores nulos de este campo corresponden a carreras canceladas por los clientes:
rides.filter(col("cancelled") == 1).count()

# Los valores reflejan que el calculo realizado por la funcion es una aproximacion
# a una distancia real:
distances.filter(col("haversine_approximation") > col("distance")).show(5)


# ## Referencias

# [Python API - datetime
# module](https://docs.python.org/2/library/datetime.html)

# [Spark Python API -
# pyspark.sql.functions.udf](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf)

# [Spark Python API -
# pyspark.sql.functions.pandas_udf](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

# [Cloudera Engineering Blog - Working with UDFs in Apache
# Spark](https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/)

# [Cloudera Engineering Blog - Use your favorite Python library on PySpark
# cluster with Cloudera Data Science
# Workbench](https://blog.cloudera.com/blog/2017/04/use-your-favorite-python-library-on-pyspark-cluster-with-cloudera-data-science-workbench/)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
