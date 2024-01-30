# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from scd2 import *
import connect as connect

# Create SparkSession
spark = SparkSession.builder.master(
    "local[1]").appName("SCD_impl").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()

cnx = connect.conn()
# a = cnx.is_connected()
# apply_initial()
apply_incremental(spark)
