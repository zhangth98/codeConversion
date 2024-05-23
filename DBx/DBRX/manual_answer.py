# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

def get_transformation_func_mapping_arrays(config_id: int, return_length: int) -> dict:
    spark = SparkSession.builder.getOrCreate()
    #Fetch config table, initial repace i0 column, and normalize transformation function data types
    df = (spark.read.table("data_management.cfg.vw_raw_to_curated_column_mapping")
        .where(f"config_id == {config_id} and active_ind == 1")
        .withColumn("transform_func_data_type", initcap("transform_func_data_type"))
        .withColumn("transform_func_data_type", when(col('transform_func_data_type').isin(['Numeric','Bit','Decimal','Integer']),'Numeric')
                                .when(col('transform_func_data_type').isin(['Timestamp','Datetime']), 'Timestamp')
                                .otherwise(col('transform_func_data_type')))
    )

    #get source column list
    raw_columns = (df.where("derived_ind=0 or (derived_ind=1 and transformation_function_id is null)") 
                    .select("source_column_name")
                    ).distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()

    # get derived source column list
    derived_raw_columns = (df.where("derived_ind == 1 and transformation_function_id is not null")        
                    .select(concat(lit("cast(null as varchar) as "), col("source_column_name")).alias("source_column_name")) 
                    ).distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()

    #get fully qualified source table name
    full_source_table_name=df.select(concat(col('source_database_name'),lit('.'),col('source_schema_name'),lit('.'),col('source_table_name'))).distinct().collect()[0][0]

    #format the source_query
    source_query='select ' + ', '.join((raw_columns + derived_raw_columns)) + ' from ' + full_source_table_name + " WHERE AUDIT_ACTIVE_ROW_IND='Y'"

    # Initialize lists to store data types, keys, and values
    d_types, f_keys, f_vals = [], [], []

    # Iterate over DataFrame rows
    for row in df.filter("transformation_function_id is not null").collect():
        # Replace placeholders with actual values in the transformation function definition
        f_def = row["transformation_function_def"].replace('i0', row["source_column_name"])
        parameter_list = row["transformation_parameter_text"]
        
        # Iterate over parameters and replace placeholders in the function definition
        for i, p in enumerate(parameter_list.split('|')):
            f_def = f_def.replace('i' + str(i + 1), p)
        
        # Append key, value, and data type to respective lists
        f_keys.append(row["target_column_name"])
        f_vals.append(f_def)
        d_types.append(row["transform_func_data_type"])            

    # Extract necessary information from DataFrame
    f_trans = {}
    for row in df.where("transformation_function_id is not null").collect():
        f_def = row["transformation_function_def"]
        for i, p in enumerate(row["transformation_parameter_text"].split('|')):
            f_def = re.sub('i' + str(i + 1), p, f_def)
        f_trans[(row["target_column_name"], row["transform_func_data_type"])] = f_def

    # Group transformations by data type
    transformations = {"Boolean": [], "Date": [], "Timestamp": [], "Numeric": [], "String": []}
    for (target_column, data_type), f_def in f_trans.items():
        data_type = data_type.capitalize()
        transformations[data_type].append((target_column, f_def))

    # Fill missing transformations with null placeholder
    for data_type, transform_list in transformations.items():
        null_placeholder = {"Boolean": "toBoolean(null())", 
                            "Date": "toDate(null())", 
                            "Timestamp": "toTimestamp(null())", 
                            "Numeric": "toDecimal(null())", 
                            "String": "toString(null())"}[data_type]
        transform_list.extend([("_" + data_type[0].lower() + str(i + 1), null_placeholder) for i in range(len(transform_list), return_length)])

    # Unpack transformations for each data type
    b_keys, b_vals = zip(*transformations["Boolean"])
    d_keys, d_vals = zip(*transformations["Date"])
    dt_keys, dt_vals = zip(*transformations["Timestamp"])
    n_keys, n_vals = zip(*transformations["Numeric"])
    s_keys, s_vals = zip(*transformations["String"])

    # Convert to string and return as a dict
    return {"source_query":source_query,
            "bkeys":';'.join(b_keys), 
            "bvals":';'.join(b_vals), 
            "dkeys":';'.join(d_keys), 
            "dvals":';'.join(d_vals), 
            "dtkeys":';'.join(dt_keys), 
            "dtvals":';'.join(dt_vals), 
            "nkeys":';'.join(n_keys), 
            "nvals":';'.join(n_vals), 
            "skeys":';'.join(s_keys), 
            "svals":';'.join(s_vals)}

# COMMAND ----------

get_transformation_func_mapping_arrays(672,40)