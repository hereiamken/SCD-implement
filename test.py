# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from scd2 import *
import connect as connect

# Create SparkSession
# spark = SparkSession.builder \
#     .appName('SparkByExamples.com') \
#     .config("spark.jars", "mysql-connector-java-8.0.13.jar")\
#     .getOrCreate()

spark = SparkSession.builder.master(
    "local[1]").appName("test").getOrCreate()

connect.conn()
# apply_initial(spark)
# apply_incremental(spark)
