# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from scd2 import *
import connect as connect

# Create SparkSession
spark = SparkSession.builder.master(
    "local[1]").appName("SCD_impl").getOrCreate()

connect.conn()
# apply_initial()
apply_incremental(spark)
