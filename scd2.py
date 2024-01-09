from common import format_columns_name, get_hash, get_suffix_name
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from mysql.connector import *
from params import *

from params import DATE_FORMAT, DEST_PATH, EOW_DATE, SOURCE_CHANGED_PATH, SOURCE_PATH

spark = SparkSession.builder.master(
    "local[1]").appName("scd_type2").getOrCreate()

window_spec = Window.orderBy("CustomerID")


def apply_initial(spark):
    df_current = spark.read.options(
        header=True, delimiter=',', inferSchema='True')\
        .csv(SOURCE_PATH)\
        .withColumn("effective_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("expiration_date", date_format(lit(EOW_DATE), DATE_FORMAT))\
        .withColumn("current_flag", lit(True))
    # .withColumn("sk_customer_id", row_number().over(window_spec))\
    df_current_renamed = format_columns_name(df_current, df_current.columns)

    df_current_renamed.show()


def apply_incremental(spark):
    df_current = spark.read.options(header=True, delimiter=',', inferSchema='True')\
        .csv(SOURCE_CHANGED_PATH)

    df_history = spark.read.options(header=True, delimiter=',', inferSchema='True')\
        .csv(DEST_PATH)\
        .withColumn("effective_date", to_date("effective_date"))\
        .withColumn("expiration_date", to_date("expiration_date"))

    # max_sk = df_history.agg({"sk_customer_id": "max"}).collect()[0][0]

    # Get current data at Destination with active and non-active data
    df_history_active = df_history.where(col("current_flag") == lit(True))
    df_history_inactive = df_history.where(col("current_flag") == lit(False))

    # Generate hash for type2 columns
    df_history_active_hash = get_suffix_name(
        get_hash(df_history_active, type2_cols), suffix="_history", append=True)
    df_current_hash = get_suffix_name(
        get_hash(df_current, type2_cols), suffix="_current", append=True)

    # Apply full outer join to df_history and current dataframes
    # Create a new column which will be used to flag records
    # 1. If hash_md5_current & hash_md5_history are same then NOCHANGE
    # 2. If CustomerID at df_current is null then DELETE
    # 3. If CustomerID at df_history is null then INSERT
    # 4. Else UPDATE
    df_merged = df_history_active_hash.join(df_current_hash,
                                            df_history_active_hash.CustomerID_history == df_current_hash.CustomerID_current, "fullouter")\
        .withColumn("Action", when(df_current_hash.hash_md5_current == df_history_active_hash.hash_md5_history, 'NOCHANGE')
                    .when(df_current_hash.CustomerID_current.isNull(), 'DELETE')
                    .when(df_history_active_hash.CustomerID_history.isNull(), 'INSERT')
                    .otherwise('UPDATE'))

    # Get data with no change
    df_nochange = get_suffix_name(df_merged.where(
        col("Action") == 'NOCHANGE'), suffix="_history", append=False).select(df_history_active.columns)

    # Get data new inserted
    df_inserted = get_suffix_name(df_merged.where(col("Action") == 'INSERT'), suffix="_current", append=False)\
        .select(df_current.columns)\
        .withColumn("effective_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("expiration_date", date_format(lit(EOW_DATE), DATE_FORMAT))\
        .withColumn("current_flag", lit(True))
    # .withColumn("row_number", row_number().over(window_spec))\
    # .withColumn("sk_customer_id", col("row_number") + max_sk)\

    # Get data has been deleted
    df_deleted = get_suffix_name(df_merged.where(col("Action") == 'DELETE'), suffix="_history", append=False)\
        .select(df_history_active.columns)\
        .withColumn("expiration_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("current_flag", lit(False))

    # Get data has been updated
    df_updated_history = get_suffix_name(df_merged.where(
        col("Action") == 'UPDATE'), suffix="_history", append=False)\
        .select(df_history_active.columns)\
        .withColumn("expiration_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("current_flag", lit(False))

    df_updated_current = get_suffix_name(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
        .select(df_current.columns)\
        .withColumn("effective_date", date_format(current_date(), DATE_FORMAT))\
        .withColumn("expiration_date", date_format(lit(EOW_DATE), DATE_FORMAT))\
        .withColumn("current_flag", lit(True))

    df_updated = df_updated_history.unionByName(df_updated_current)

    df_final = df_history_inactive\
        .unionByName(df_nochange)\
        .unionByName(df_deleted)\
        .unionByName(df_inserted)\
        .unionByName(df_updated)

    df_final.write.mode('overwrite')\
        .option("header", True)\
        .option("delimiter", ",")\
        .csv(TEMP_PATH)

    print(df_final.count())
