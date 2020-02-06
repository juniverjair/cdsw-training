
# ## Overview



# Spark SQL admite varios tipos de columnas y proporciona una variedad de funciones
# y métodos de columna que se pueden aplicar a cada tipo. En este módulo nosotros
# demostrar cómo transformar columnas DataFrame de varios tipos.


# * Trabajando con columnas numéricas
# * Trabajar con columnas de cadena
# * Trabajar con columnas de fecha y hora
# * Trabajar con columnas booleanas


# ## Spark SQL Data Types

# * Spark SQL los tipos de dato están en el modulo `pyspark.sql.types` 

# * Spark SQL soporta los siguientes tipos:
#   * NullType
#   * StringType
#   * Byte array data type
#     * BinaryType
#   * BooleanType
#   * Integer data types
#     * ByteType
#     * ShortType
#     * IntegerType
#     * LongType
#   * Fixed-point data type
#     * DecimalType
#   * Floating-point data types
#     * FloatType
#     * DoubleType
#   * Date and time data types
#     * DateType
#     * TimestampType

# * Soporta tambien tipos complejos (collection) types:
#   * ArrayType
#   * MapType
#   * StructType

# * Spark SQL provee funciones para aplicar sobre los tipos de datos



# ## inicializacion de spark


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("columns").getOrCreate()

# Leer las estructuras HDFS:
rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv("/duocar/raw/riders/", header=True, inferSchema=True)





# ## Columnas numericas

# ### Example 1: Converting ride distance from meters to miles

from pyspark.sql.functions import col, round
rides \
  .select("distance", round(col("distance") / 1609.344, 2).alias("distance_in_miles")) \
  .show(5)


# *  1 mile = 1609.344 meters.
# *  `round` 2 posiciones decimales.
# * alias  `alias` method para renombrar columnas.

#  `withColumn` :
rides \
  .withColumn("distance_in_miles", round(col("distance") / 1609.344, 2)) \
  .printSchema()

# Reemplazar la columna `withColumn` :
rides \
  .withColumn("distance", round(col("distance") / 1609.344, 2)) \
  .printSchema()

# ### Convertir numero a un string

# Usar funcion `format_string` ceros a la izquierda del campo id `id` :
from pyspark.sql.functions import format_string
rides \
  .withColumn("id_fixed", format_string("%010d", "id")) \
  .select("id", "id_fixed") \
  .show(5)

# **Nota:** Hemos usado el formato de  [printf format
# string](https://en.wikipedia.org/wiki/Printf_format_string) `%010d` 
# para formatear la variable .

# ### Example 3: un flag integer a boleano

# uso de expresion booleana:
riders \
  .withColumn("student_boolean", col("student") == 1) \
  .select("student", "student_boolean") \
  .show(5)
  
# Usando una expresion booleana:
riders \
  .withColumn("student_boolean", col("student") == 1) \
  .select("student", "student_boolean") \
  .printSchema() 

# Uso del metodo  `cast` :
riders \
  .withColumn("student_boolean", col("student").cast("boolean")) \
  .select("student", "student_boolean") \
  .show(5)


# ## Trabajando con columnas cadena

# ### Example 4: Normalizar una columna 

# Uso de `trim` y `upper` para normalizar la columna `riders.sex`:
from pyspark.sql.functions import trim, upper
riders \
  .withColumn("gender", upper(trim(col("sex")))) \
  .select("sex", "gender") \
  .show(5)

# ### Ejemplo : Extraer una subcadena de una columna

# The [Census Block Group](https://en.wikipedia.org/wiki/Census_block_group) is
#  [Link espanol] https://learn.arcgis.com/es/related-concepts/united-states-census-geography.htm
# the first 12 digits of the [Census
# Block](https://en.wikipedia.org/wiki/Census_block).  usar la funcion `substring`
# para extraer el grupo de bloque censal de la columna  `riders.home_block`:
from pyspark.sql.functions import substring
riders \
  .withColumn("home_block_group", substring("home_block", 1, 12)) \
  .select("home_block", "home_block_group") \
  .show(5)


#  `regexp_extract` extraer el group de bloque censal via
# una expresion regular:
from pyspark.sql.functions import regexp_extract
riders \
  .withColumn("home_block_group", regexp_extract("home_block", "^(d{12}).*", 1)) \
  .select("home_block", "home_block_group") \
  .show(5)

# **Nota:** en el llamado `regexp_extract` el tercer parametro es el
# grupo de captura.


# ## Trabajando con fechas y timestamps 

# ### Convertir timestamp a date

# Notar que las fechas  `riders.birth_date` y  `riders.start_date` son leidos como timestamps:
riders.select("birth_date", "start_date").show(5)

# Usar  `cast` para convertir `riders.birth_date` a date:

riders \
  .withColumn("birth_date_fixed", col("birth_date").cast("date")) \
  .select("birth_date", "birth_date_fixed") \
  .show(5)
  
# ver el tipo de dato al aplicar el metodo `cast` :
riders \
  .withColumn("birth_date_fixed", col("birth_date").cast("date")) \
  .select("birth_date", "birth_date_fixed") \
  .printSchema()

# Como alternativa usar `to_date` :
from pyspark.sql.functions import to_date
riders \
  .withColumn("birth_date_fixed", to_date("birth_date")) \
  .select("birth_date", "birth_date_fixed") \
  .show(5)

# ### Convertir cadena a timestamp

# ver que el campo lo esta viendo como cadena `rides.date_time` :
rides.printSchema()

# Use the `cast` method to convert it to a timestamp:
rides \
  .withColumn("date_time_fixed", col("date_time").cast("timestamp")) \
  .select("date_time", "date_time_fixed") \
  .show(5)

# Como alternativa usar la funcion `to_timestamp` :
from pyspark.sql.functions import to_timestamp
rides \
  .withColumn("date_time_fixed", to_timestamp("date_time", format="yyyy-MM-dd HH:mm")) \
  .select("date_time", "date_time_fixed") \
  .show(5)

# ###  calcular la edad de cada conductor

# usar las funciones  `current_date` y `months_between` para calcular edad :
from pyspark.sql.functions import current_date, months_between, floor
riders \
  .withColumn("today", current_date()) \
  .withColumn("age", floor(months_between("today", "birth_date") / 12)) \
  .select("birth_date", "today", "age") \
  .show(5)

# **Nota:** Spark hace un cast implicito sobre uno de los campos `birth_date` o `today` 
# como sea necesario
# Probablemente sea mas seguro usar un cast explicito a una de las columnas
#  antes de calcular el numero de meses entre esas columnas 


# ##  Trabajando con expresiones booleanas

# ### Predefiniendo una columna booleana

# puedes predefinir una expresion de columna :
studentFilter = col("student") == 1
type(studentFilter)
print(studentFilter)

# crear una nueva columna:
riders \
  .withColumn("student_boolean", studentFilter) \
  .select("student", "student_boolean") \
  .show(5)

# filtrar una columna:
riders \
  .filter(studentFilter) \
  .select("student") \
  .show(5)

# ### Ejemplo 11: trabajar con multiples valores booleanos 

# Predefinir expresiones booleanas:
studentFilter = col("student") == 1
maleFilter = col("sex") == "male"

# usar operadores (`&`) :
riders.select("student", "sex", studentFilter & maleFilter).show(15)

# Usar operador or (`|`) operator:
riders.select("student", "sex", studentFilter | maleFilter).show(15)

# **Important:** parsear las expresiones boleanas  
# no es muy avanzando, usar parentesis en tus expresiones


# Ver como trata los nulos :
# * true & null = null
# * false & null = false
# * true | null = true
# * false | null = null

# ### Ejemplo 12: usando expresiones multiples en un filtro.

# Usar `&`  AND:
riders.filter(maleFilter & studentFilter).select("student", "sex").show(5)

# equivalente a 
riders.filter(maleFilter).filter(studentFilter).select("student", "sex").show(5)

# Usar `|` uso para OR:
riders.filter(maleFilter | studentFilter).select("student", "sex").show(5)

# Tener cuidado con los valores nulos (null) :
riders.select("sex").distinct().show()
riders.filter(col("sex") != "male").select("sex").distinct().show()





# ## Referencias

# [Spark Python API - pyspark.sql.types
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)

# [Spark Python API - pyspark.sql.DataFrame
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Spark Python API - pyspark.sql.Column
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
