from common import format_columns, format_columns_name, get_hash, get_suffix_name
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from mysql.connector import *
from connect import PASSWORD, USER, process_many_rows, process_many_rows_1, process_row
from params import *

from params import DATE_FORMAT, DEST_PATH, EOW_DATE, SOURCE_CHANGED_PATH, SOURCE_PATH

spark = SparkSession.builder\
    .appName('scd_type2')\
    .config("spark.jars", "mysql-connector-j-8.2.0.jar")\
    .getOrCreate()

window_spec = Window.orderBy("CustomerID")


def apply_initial():
    df_current = spark.read.options(
        header=True, delimiter=',', inferSchema='True')\
        .csv(SOURCE_PATH)\
        .withColumn("sk_customerid", row_number().over(window_spec))\
        .withColumn("effective_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("expiration_date", date_format(lit(EOW_DATE), DATE_FORMAT))\
        .withColumn("current_flag", lit(True))
    # .withColumn("sk_customer_id", row_number().over(window_spec))\
    df_current_renamed = format_columns_name(df_current, df_current.columns)

    df_current_renamed.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/scd_impl") \
        .option("dbtable", "customer") \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .save()


def apply_incremental(spark):
    df_columns = spark.read.options(header=True, delimiter=',', inferSchema='True')\
        .csv(SOURCE_PATH).columns
    df_columns.append('effective_date')
    df_columns.append('expiration_date')
    df_columns.append('current_flag')
    df_columns.append('sk_customerid')

    columns = format_columns(df_columns)

    df_current = spark.read.options(header=True, delimiter=',', inferSchema='True')\
        .csv(SOURCE_CHANGED_PATH)

    df_current_renamed = format_columns_name(df_current, df_current.columns)

    df_history = spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/scd_impl") \
        .option("dbtable", "customer") \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .load()\
        .select(columns)

    # Keep track of the maximum surrogate key in the history table
    # as it will be used to create a surrogate key for newly inserted and updated records.
    max_history_sk = df_history.agg({"sk_customerid": "max"}).collect()[0][0]

    # Get current data at Destination with active and non-active data
    df_history_active = df_history.where(col("current_flag") == lit(True))
    df_history_inactive = df_history.where(col("current_flag") == lit(False))

    # Generate hash for type2 columns
    df_history_active_hash = get_suffix_name(
        get_hash(df_history_active, type2_cols), suffix="_history", append=True)
    df_current_hash = get_suffix_name(
        get_hash(df_current_renamed, type2_cols), suffix="_current", append=True)

    # Apply full outer join to df_history and current dataframes
    # Create a new column which will be used to flag records
    # 1. If hash_md5_current & hash_md5_history are same then NOCHANGE
    # 2. If CustomerID at df_current is null then DELETE
    # 3. If CustomerID at df_history is null then INSERT
    # 4. Else UPDATE
    df_merged = df_history_active_hash.join(df_current_hash,
                                            df_history_active_hash.customerid_history == df_current_hash.customerid_current, "fullouter")\
        .withColumn("Action", when(df_current_hash.hash_md5_current == df_history_active_hash.hash_md5_history, 'NOCHANGE')
                    .when(df_current_hash.customerid_current.isNull(), 'DELETE')
                    .when(df_history_active_hash.customerid_history.isNull(), 'INSERT')
                    .otherwise('UPDATE'))

    # Get data with no change
    df_nochange = get_suffix_name(df_merged.where(
        col("Action") == 'NOCHANGE'), suffix="_history", append=False).select(df_history_active.columns)

    # Get data new inserted
    df_inserted = get_suffix_name(df_merged.where(col("Action") == 'INSERT'), suffix="_current", append=False)\
        .select(df_current_renamed.columns)\
        .withColumn("effective_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("expiration_date", date_format(lit(EOW_DATE), DATE_FORMAT))\
        .withColumn("row_number", row_number().over(window_spec))\
        .withColumn("sk_customerid", col("row_number") + max_history_sk)\
        .withColumn("current_flag", lit(True))\
        .drop("row_number")

    if df_inserted.count() > 0:
        process_many_rows(df=df_inserted, table_name='customer',
                          change_type='INSERT')

    max_insert_sk = df_inserted.agg({"sk_customerid": "max"}).collect()[0][0]
    # Get data has been deleted
    df_deleted = get_suffix_name(df_merged.where(col("Action") == 'DELETE'), suffix="_history", append=False)\
        .select(df_history_active.columns)\
        .withColumn("expiration_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("current_flag", lit(False))

    if df_deleted.count() > 0:
        process_many_rows(df=df_deleted, table_name='customer',
                          change_type='UPDATE')

    # Get data has been updated
    df_updated_history = get_suffix_name(df_merged.where(
        col("Action") == 'UPDATE'), suffix="_history", append=False)\
        .select(df_history_active.columns)\
        .withColumn("effective_date", date_format("effective_date", DATE_FORMAT))\
        .withColumn("expiration_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("current_flag", lit(False))

    if df_updated_history.count() > 0:
        process_many_rows(df=df_updated_history,
                          table_name='customer', change_type='INSERT')

    df_updated_current = get_suffix_name(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
        .select(df_current_renamed.columns)\
        .withColumn("effective_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("expiration_date", date_format(lit(EOW_DATE), DATE_FORMAT))\
        .withColumn("row_number", row_number().over(window_spec))\
        .withColumn("sk_customerid", col("row_number") + max_insert_sk)\
        .withColumn("current_flag", lit(True))\
        .drop("row_number")

    if df_updated_current.count() > 0:
        process_many_rows(df=df_updated_current,
                          table_name='customer', change_type='UPDATE')

    df_updated = df_updated_history.unionByName(df_updated_current)

    df_final = df_history_inactive\
        .unionByName(df_nochange)\
        .unionByName(df_deleted)\
        .unionByName(df_inserted)\
        .unionByName(df_updated)

    print(df_final.count())
