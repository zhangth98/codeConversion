# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_transformation_func_mapping_arrays(config_id, return_length=8):
    spark = SparkSession.builder.getOrCreate()
    
    # Get raw columns
    raw_columns_df = spark.sql(f"""
        SELECT source_column_name as raw_columns
        FROM cfg.vw_raw_to_curated_column_mapping 
        WHERE config_id={config_id} AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL))
    """)

    raw_columns=[f"[{cName}]" for cName in raw_columns_df.select("raw_columns").rdd.flatMap(lambda x: x).collect()]

    derived_columns_df = spark.sql(f"""
        SELECT source_column_name as derived_columns
        FROM cfg.vw_raw_to_curated_column_mapping 
        WHERE config_id={config_id} AND active_ind=1 AND derived_ind=1 AND transformation_function_id IS NOT NULL
    """)

    derived_columns=[f"[{cName}]" for cName in derived_columns_df.select("derived_columns").rdd.flatMap(lambda x: x).collect()]

    source_columns=','.join(raw_columns + derived_columns)
    
    source_database_name,source_schema_name,source_table_name=spark.sql(f"""
            select source_database_name,source_schema_name,source_table_name
            from data_management.cfg.source_to_output_config
            where active_ind=1
            and config_id={config_id}""").collect()[0]
    # Get source query
    source_query = f"SELECT {source_columns} FROM {source_database_name}.{source_schema_name}.{source_table_name} WHERE AUDIT_ACTIVE_ROW_IND='Y'"
    
    # Initialize variables
    bkeys, bvals, dkeys, dvals, dtkeys, dtvals, nkeys, nvals, skeys, svals = '', '', '', '', '', '', '', '', '', ''
    b_count, d_count, dt_count, n_count, s_count = 0, 0, 0, 0, 0
    
    # Get function definitions
    df = spark.sql(f"""
        WITH CTE AS (
            SELECT custom_column_mapping_id, source_column_name, replace(transformation_function_def,'i0', source_column_name) as f_definition, transformation_parameter_text as f_parameter_text, transform_func_data_type as transform_data_type
            FROM cfg.vw_raw_to_curated_column_mapping
            WHERE active_ind=1 AND config_id={config_id} AND transformation_function_id IS NOT NULL
        )
        SELECT source_column_name, f_definition, f_parameter_text, transform_data_type
        FROM CTE
        ORDER BY custom_column_mapping_id
    """)
    
    for row in df.collect():
        column_name, f_def, p_list, p_dtype = row
        f_def = f_def.replace('i0', column_name)
        p_list = p_list.split('|')
        
        if p_dtype == 'Boolean':
            b_count += 1
            bkeys += f';"{column_name}"'
            bvals += f';"{f_def}"'
        elif p_dtype == 'Date':
            d_count += 1
            dkeys += f';"{column_name}"'
            dvals += f';"{f_def}"'
        elif p_dtype in ('Timestamp', 'DateTime'):
            dt_count += 1
            dtkeys += f';"{column_name}"'
            dtvals += f';"{f_def}"'
        elif p_dtype in ('Numeric', 'Decimal', 'Integer', 'Bit'):
            n_count += 1
            nkeys += f';"{column_name}"'
            nvals += f';"{f_def}"'
        elif p_dtype == 'String':
            s_count += 1
            skeys += f';"{column_name}"'
            svals += f';"{f_def}"'
    
    # Pad variables
    while b_count < return_length:
        b_count += 1
        bkeys += f';"_b{b_count}"'
        bvals += ';"toBoolean(null())"'
    while d_count < return_length:
        d_count += 1
        dkeys += f';"_d{d_count}"'
        dvals += ';"toDate(null())"'
    while dt_count < return_length:
        dt_count += 1
        dtkeys += f';"_dt{dt_count}"'
        dtvals += ';"toTimestamp(null())"'
    while n_count < return_length:
        n_count += 1
        nkeys += f';"_n{d_count}"'
        nvals += ';"toDecimal(null())"'
    while s_count < return_length:
        s_count += 1
        skeys += f';"_s{d_count}"'
        svals += ';"toString(null())"'
    
    # Remove semi-column from the beginning
    bkeys = bkeys[1:]
    bvals = bvals[1:]
    dkeys = dkeys[1:]
    dvals = dvals[1:]
    dtkeys = dtkeys[1:]
    dtvals = dtvals[1:]
    nkeys = nkeys[1:]
    nvals = nvals[1:]
    skeys = skeys[1:]
    svals = svals[1:]
    
    return {'source_query': source_query, 'bkeys': bkeys, 'bvals': bvals, 'dkeys': dkeys, 'dvals': dvals, 'dtkeys': dtkeys, 'dtvals': dtvals, 'nkeys': nkeys, 'nvals': nvals, 'skeys': skeys, 'svals': svals}

# COMMAND ----------

get_transformation_func_mapping_arrays(672,40)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_transformation_func_mapping_arrays(config_id, return_length=8):
    spark = SparkSession.builder.getOrCreate()
    
    # Get raw columns
    raw_columns = spark.sql(f"""
        SELECT string_agg('[' + source_column_name + ']', ',') as raw_columns
        FROM cfg.vw_raw_to_curated_column_mapping 
        WHERE config_id={config_id} AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL))
    """).collect()[0].raw_columns
    
    # Get source query
    source_query = spark.sql(f"""
        SELECT * FROM (
            SELECT {raw_columns}, {', '.join([f"cast(Null as varchar) as {col}" for col in source_columns])}
            FROM {source_database_name}.{source_schema_name}.{source_table_name}
            WHERE AUDIT_ACTIVE_ROW_IND='Y'
        ) WHERE 1=1
        AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NOT NULL))
    """).collect()[0].source_query
    
    # Initialize variables
    bkeys, bvals, dkeys, dvals, dtkeys, dtvals, nkeys, nvals, skeys, svals = '', '', '', '', '', '', '', '', '', ''
    b_count, d_count, dt_count, n_count, s_count = 0, 0, 0, 0, 0
    
    # Get function definitions
    df = spark.sql(f"""
        WITH CTE AS (
            SELECT custom_column_mapping_id, source_column_name, replace(transformation_function_def,'i0', source_column_name) as f_definition, transformation_parameter_text as f_parameter_text, transform_func_data_type as transform_data_type
            FROM cfg.vw_raw_to_curated_column_mapping
            WHERE active_ind=1 AND config_id={config_id} AND transformation_function_id IS NOT NULL
        )
        SELECT source_column_name, f_definition, f_parameter_text, transform_data_type
        FROM CTE
        ORDER BY custom_column_mapping_id
    """)
    
    for row in df.collect():
        column_name, f_def, p_list, p_dtype = row
        f_def = f_def.replace('i0', column_name)
        p_list = p_list.split('|')
        
        if p_dtype == 'Boolean':
            b_count += 1
            bkeys += f';"{column_name}"'
            bvals += f';"{f_def}"'
        elif p_dtype == 'Date':
            d_count += 1
            dkeys += f';"{column_name}"'
            dvals += f';"{f_def}"'
        elif p_dtype in ('Timestamp', 'DateTime'):
            dt_count += 1
            dtkeys += f';"{column_name}"'
            dtvals += f';"{f_def}"'
        elif p_dtype in ('Numeric', 'Decimal', 'Integer', 'Bit'):
            n_count += 1
            nkeys += f';"{column_name}"'
            nvals += f';"{f_def}"'
        elif p_dtype == 'String':
            s_count += 1
            skeys += f';"{column_name}"'
            svals += f';"{f_def}"'
    
    # Pad variables
    while b_count < return_length:
        b_count += 1
        bkeys += f';"_b{b_count}"'
        bvals += ';"toBoolean(null())"'
    while d_count < return_length:
        d_count += 1
        dkeys += f';"_d{d_count}"'
        dvals += ';"toDate(null())"'
    while dt_count < return_length:
        dt_count += 1
        dtkeys += f';"_dt{dt_count}"'
        dtvals += ';"toTimestamp(null())"'
    while n_count < return_length:
        n_count += 1
        nkeys += f';"_n{d_count}"'
        nvals += ';"toDecimal(null())"'
    while s_count < return_length:
        s_count += 1
        skeys += f';"_s{d_count}"'
        svals += ';"toString(null())"'
    
    # Remove semi-column from the beginning
    bkeys = bkeys[1:]
    bvals = bvals[1:]
    dkeys = dkeys[1:]
    dvals = dvals[1:]
    dtkeys = dtkeys[1:]
    dtvals = dtvals[1:]
    nkeys = nkeys[1:]
    nvals = nvals[1:]
    skeys = skeys[1:]
    svals = svals[1:]
    
    return {'source_query': source_query, 'bkeys': bkeys, 'bvals': bvals, 'dkeys': dkeys, 'dvals': dvals, 'dtkeys': dtkeys, 'dtvals': dtvals, 'nkeys': nkeys, 'nvals': nvals, 'skeys': skeys, 'svals': svals}

# COMMAND ----------


