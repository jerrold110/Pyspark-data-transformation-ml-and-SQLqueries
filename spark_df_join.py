import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,BooleanType,DateType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "4g") \
    .appName('read-csv') \
    .getOrCreate()

flights = spark.read.csv("flights_small.csv", header=True)
planes = spark.read.csv("planes.csv", header=True)

# Rename year column so they do not conflict with flights.year
planes = planes.withColumnRenamed("year", "plane_year")


model_data = flights.join(planes, on="tailnum", how="leftouter")

model_data.write.csv('model_dataset.csv')
#model_data.repartition(1).write.format('csv').save("myfile.csv",header='true')
