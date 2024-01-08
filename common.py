from pyspark.sql.functions import *


def get_suffix_name(df, suffix, append):
    """
    input:
        df: dataframe
        suffix: suffix to be appended to column name
        append: boolean value 
                if true append suffix else remove suffix

    output:
        df: df with renamed column
    """
    if append:
        new_column_names = list(map(lambda x: x+suffix, df.columns))
    else:
        new_column_names = list(
            map(lambda x: x.replace(suffix, ""), df.columns))
    return df.toDF(*new_column_names)


def get_hash(df, keys_list):
    """
    input:
        df: dataframe
        key_list: list of columns to be hashed    
    output:
        df: df with hashed column
    """
    columns = [col(column) for column in keys_list]
    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", md5(lit(1)))
