import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.getOrCreate()

flights = spark.read.csv("flights_small.csv", header=True)
planes = spark.read.csv("planes.csv", header=True)

# Rename year column so they do not conflict with flights.year
planes = planes.withColumnRenamed("year", "plane_year")

model_data = flights.join(planes, on="tailnum", how="leftouter")

# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("air_time", model_data.air_time.cast("integer"))
model_data = model_data.withColumn("month", model_data.month.cast("integer"))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast("integer"))
model_data = model_data.withColumn("year", model_data.year.cast("integer"))

# Create the column plane_age
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)

# Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))
