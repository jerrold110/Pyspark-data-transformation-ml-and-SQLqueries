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

# Create is_late and label
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))

# Remove missing values from arr_delay,dep_delay,air_time,plane_year
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

from pyspark.ml.feature import StringIndexer,OneHotEncoder,VectorAssembler

carr_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_index")
carr_encoder = OneHotEncoder(inputCol="carrier_index", outputCol="carrier_fact")

dest_indexer = StringIndexer(inputCol="dest", outputCol="dest_index")
dest_encoder = OneHotEncoder(inputCol="dest_index", outputCol="dest_fact")

# VectorAssembler
# A feature transformer that merges multiple columns into a vector column
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], outputCol="features")

from pyspark.ml import Pipeline

# Make the pipeline
# One-hot encoding, vector assembler
flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])
piped_data = flights_pipe.fit(model_data).transform(model_data)

# Split the data into training and test sets
training, test = piped_data.randomSplit([.6, .4])

import numpy as np
from pyspark.ml.classification import LogisticRegression
import pyspark.ml.evaluation as evals
import pyspark.ml.tuning as tune

lr = LogisticRegression()

evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")

grid = tune.ParamGridBuilder()
# Add the hyperparameters regParam and ElasticNetParam for regularisation
# Elasticnet is combination of L1 and L2 regression
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])
# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)

# Fitting and evaluation cross validation models on test set
models = cv.fit(training)
best_lr = models.bestModel
print(best_lr)

test_results = best_lr.transform(test)
print(f'AUC score of best model on test set: {evaluator.evaluate(test_results)}')
