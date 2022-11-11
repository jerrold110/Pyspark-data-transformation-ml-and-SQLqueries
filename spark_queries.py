import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.getOrCreate()

# Define schema
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,BooleanType,DateType

flight_schema = StructType([ \
    StructField("year",IntegerType(),True), \
    StructField("month",IntegerType(),True), \
    StructField("day",IntegerType(),True), \
    StructField("dep_time",IntegerType(),True), \
    StructField("dep_delay",IntegerType(),True), \
    StructField("arr_time",IntegerType(),True), \
    StructField("arr_delay",IntegerType(),True), \
    StructField("carrier",StringType(),True), \
    StructField("tailnum",StringType(),True), \
    StructField("flight",IntegerType(),True), \
    StructField("origin",StringType(),True), \
    StructField("dest",StringType(),True), \
    StructField("air_time",IntegerType(),True), \
    StructField("distance",IntegerType(),True), \
    StructField("hour",IntegerType(),True), \
    StructField("minute",IntegerType(),True), 
  ])

# Read in the airports data
flights = spark.read.csv("flights_small.csv", header=True, schema=flight_schema)
#flights = flights.withColumn("distance", flights.distance.cast(IntegerType()))

print(flights.columns)
print(flights.count())
print(flights.printSchema())

# SQL queries
#flights.createGlobalTempView("flights")
flights.createTempView("flights")
#query = "select * from global_temp.flights limit 10"
query = "select * from flights limit 10"
flights10 = spark.sql(query)
flights10.show()

query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"
flights_query = spark.sql(query)
flights_query.show()

# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")
long_flights2 = flights.filter(flights.distance > 1000)
long_flights1.show()
long_flights2.show()

# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)
speed1.show()

# Find the shortest flight in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()

# Count number of flights each plane made
flights.groupBy("tailnum").count().show()



