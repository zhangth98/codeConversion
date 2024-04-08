# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re

def get_transformation_func_mapping_arrays(config_id: int, return_length: int) -> dict:
    spark = SparkSession.builder.getOrCreate()

    df = (spark.read.table("data_management.cfg.vw_raw_to_curated_column_mapping")
        .where(f"config_id == {config_id} and active_ind == 1")
        .withColumn("transformation_function_def", replace(col("transformation_function_def"),lit('i0'), col("source_column_name")))
    )

    raw_columns = (df.where("derived_ind=0 or (derived_ind=1 and transformation_function_id is null)") 
                    .select("source_column_name")
                    ).distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()

    derived_raw_columns = (df.where("derived_ind == 1 and transformation_function_id is not null")        
                    .select(concat(lit("cast(null as varchar) as "), col("source_column_name")).alias("source_column_name")) 
                    ).distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()

    full_source_table_name=df.select(concat(col('source_database_name'),lit('.'),col('source_schema_name'),lit('.'),col('source_table_name'))).distinct().collect()[0][0]

    source_query='select ' + ', '.join((raw_columns + derived_raw_columns)) + ' from ' + full_source_table_name + " WHERE AUDIT_ACTIVE_ROW_IND='Y'"

    bkeys,bvals,dkeys,dvals, dtkeys, dtvals, nkeys, nvals, skeys,svals = "","","","","","","","","",""
    b_count, d_count,dt_count,n_count,s_count = 0,0,0,0,0   

    d_types, f_keys, f_vals=[],[],[]

    for row in df.where("transformation_function_id is not null").collect():
        f_def=re.sub('i0', row["source_column_name"], row["transformation_function_def"])
        target_column=row["target_column_name"]
        p_type = row["transform_func_data_type"]
        parameter_list=row["transformation_parameter_text"]
        for p in enumerate(parameter_list.split('|')):
            f_def=re.sub('i'+ str(p[0]+1), p[1], f_def)
        d_types += [p_type]
        f_keys += [target_column]
        f_vals += [f_def]

    f_trans=dict(zip(d_types, zip(f_keys, f_vals)))

    b_keys=[f_trans[k][0] for k in f_trans.keys() if k.capitalize()=='Boolean']
    b_vals=[f_trans[k][1] for k in f_trans.keys() if k.capitalize()=='Boolean']
    d_keys=[f_trans[k][0] for k in f_trans.keys() if k.capitalize()=='Date']
    d_vals=[f_trans[k][1] for k in f_trans.keys() if k.capitalize()=='Date']
    dt_keys=[f_trans[k][0] for k in f_trans.keys() if k.capitalize() in ('Timestamp','Datetime')]
    dt_vals=[f_trans[k][1] for k in f_trans.keys() if k.capitalize() in ('Timestamp','Datetime')]
    n_keys=[f_trans[k][0] for k in f_trans.keys() if k.capitalize() in ("Numeric", "Decimal", "Integer", "Bit")]
    n_vals=[f_trans[k][1] for k in f_trans.keys() if k.capitalize() in ("Numeric", "Decimal", "Integer", "Bit")]
    s_keys=[f_trans[k][0] for k in f_trans.keys() if k.capitalize()=='String']
    s_vals=[f_trans[k][1] for k in f_trans.keys() if k.capitalize()=='String']

    b_keys += [f"_b{i+1}" for i in range(len(b_keys), return_length)]
    b_vals += ["toBoolean(null())" for i in range(len(b_vals), return_length)]

    d_keys += [f"_d{i+1}" for i in range(len(d_keys), return_length)]
    d_vals += ["toDate(null())" for i in range(len(d_vals), return_length)]

    dt_keys += [f"_dt{i+1}" for i in range(len(dt_keys), return_length)]
    dt_vals += ["toTimestamp(null())" for i in range(len(dt_vals), return_length)]

    n_keys += [f"_n{i+1}" for i in range(len(n_keys), return_length)]
    n_vals += ["toDecimal(null())" for i in range(len(n_vals), return_length)]

    s_keys += [f"_s{i+1}" for i in range(len(s_keys), return_length)]
    s_vals += ["toString(null())" for i in range(len(s_vals), return_length)]

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

# COMMAND ----------


