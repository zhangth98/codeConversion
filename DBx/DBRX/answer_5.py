# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

def get_transformation_func_mapping_arrays(config_id, return_length=8):
    # Initialize variables
    ret_source_query = ''
    raw_columns = ''
    map_construct = '"{x}"'
    ret_bkeys, ret_bvals, ret_dkeys, ret_dvals, ret_dtkeys, ret_dtvals, ret_nkeys, ret_nvals, ret_skeys, ret_svals = '', '', '', '', '', '', '', '', '', ''
    b_count, d_count, dt_count, n_count, s_count = 0, 0, 0, 0, 0

    # Get raw columns
    raw_columns_df = spark.sql("SELECT source_column_name \
                                FROM cfg.vw_raw_to_curated_column_mapping \
                                WHERE config_id=" + str(config_id) + " AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL))")

    raw_columns=[f"[{cName}]" for cName in raw_columns_df.select("source_column_name").rdd.flatMap(lambda x: x).collect()]

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
    ret_source_query = f"SELECT {source_columns} FROM {source_database_name}.{source_schema_name}.{source_table_name} WHERE AUDIT_ACTIVE_ROW_IND='Y'"

    # Get transformation function definitions
    cte_df = spark.sql("WITH CTE AS \
                        (SELECT custom_column_mapping_id, source_column_name, replace(transformation_function_def,'i0', source_column_name) as f_definition, transformation_parameter_text as f_parameter_text, transform_func_data_type as transform_data_type \
                         FROM cfg.vw_raw_to_curated_column_mapping \
                         WHERE active_ind=1 AND config_id=" + str(config_id) + " AND transformation_function_id IS NOT NULL) \
                        SELECT source_column_name, f_definition, f_parameter_text, transform_data_type \
                        FROM CTE \
                        ORDER BY custom_column_mapping_id")

    for row in cte_df.collect():
        column_name, f_def, p_list, p_dtype = row
        p_list = re.sub(r'[i](\d+)', r'c_name\1, c_pos\1', p_list)
        c_name_pos = re.findall(r'c_name(\d+), c_pos(\d+)', p_list)

        for c_name, c_pos in c_name_pos:
            f_def = f_def.replace('i' + c_pos, c_name)

        if p_dtype == 'Boolean':
            b_count += 1
            ret_bkeys += ';' + map_construct.format(x=column_name)
            ret_bvals += ';' + map_construct.format(x=f_def)
        elif p_dtype == 'Date':
            d_count += 1
            ret_dkeys += ';' + map_construct.format(x=column_name)
            ret_dvals += ';' + map_construct.format(x=f_def)
        elif p_dtype in ('Timestamp', 'DateTime'):
            dt_count += 1
            ret_dtkeys += ';' + map_construct.format(x=column_name)
            ret_dtvals += ';' + map_construct.format(x=f_def)
        elif p_dtype in ('Numeric', 'Decimal', 'Integer', 'Bit'):
            n_count += 1
            ret_nkeys += ';' + map_construct.format(x=column_name)
            ret_nvals += ';' + map_construct.format(x=f_def)
        elif p_dtype == 'String':
            s_count += 1
            ret_skeys += ';' + map_construct.format(x=column_name)
            ret_svals += ';' + map_construct.format(x=f_def)

    # Pad variables to return_length
    while b_count < return_length:
        b_count += 1
        ret_bkeys += ';"_b' + str(b_count) + '"'
        ret_bvals += ';"toBoolean(null())"'

    while d_count < return_length:
        d_count += 1
        ret_dkeys += ';"_d' + str(d_count) + '"'
        ret_dvals += ';"toDate(null())"'

    while dt_count < return_length:
        dt_count += 1
        ret_dtkeys += ';"_dt' + str(dt_count) + '"'
        ret_dtvals += ';"toTimestamp(null())"'

    while n_count < return_length:
        n_count += 1
        ret_nkeys += ';"_n' + str(n_count) + '"'
        ret_nvals += ';"toDecimal(null())"'

    while s_count < return_length:
        s_count += 1
        ret_skeys += ';"_s' + str(s_count) + '"'
        ret_svals += ';"toString(null())"'

    # Remove semi-colon from the beginning
    ret_bkeys = re.sub(r'^;', '', ret_bkeys)
    ret_bvals = re.sub(r'^;', '', ret_bvals)
    ret_dkeys = re.sub(r'^;', '', ret_dkeys)
    ret_dvals = re.sub(r'^;', '', ret_dvals)
    ret_dtkeys = re.sub(r'^;', '', ret_dtkeys)
    ret_dtvals = re.sub(r'^;', '', ret_dtvals)
    ret_nkeys = re.sub(r'^;', '', ret_nkeys)
    ret_nvals = re.sub(r'^;', '', ret_nvals)
    ret_skeys = re.sub(r'^;', '', ret_skeys)
    ret_svals = re.sub(r'^;', '', ret_svals)

    # Return variables with name changed
    return ret_source_query, ret_bkeys, ret_bvals, ret_dkeys, ret_dvals, ret_dtkeys, ret_dtvals, ret_nkeys, ret_nvals, ret_skeys, ret_svals

# COMMAND ----------

get_transformation_func_mapping_arrays(672,40)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

def get_transformation_func_mapping_arrays(config_id, return_length=8):
    # Initialize variables
    ret_source_query = ''
    raw_columns = ''
    map_construct = '"{x}"'
    ret_bkeys, ret_bvals, ret_dkeys, ret_dvals, ret_dtkeys, ret_dtvals, ret_nkeys, ret_nvals, ret_skeys, ret_svals = '', '', '', '', '', '', '', '', '', ''
    b_count, d_count, dt_count, n_count, s_count = 0, 0, 0, 0, 0

    # Get raw columns
    raw_columns_df = spark.sql("SELECT string_agg(cast('[' + source_column_name +']' as varchar(max)), ',') within group (order by custom_column_mapping_id asc) \
                                FROM cfg.vw_raw_to_curated_column_mapping \
                                WHERE config_id=" + str(config_id) + " AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL))")
    raw_columns = raw_columns_df.collect()[0][0]

    # Get source query
    if len(raw_columns) > 0:
        ret_source_query_df = spark.sql("SELECT 'SELECT ' + " + raw_columns + " + ', ' + string_agg(cast(replace('cast(Null as varchar) as {x}', '{x}', source_column_name) as varchar(max)), ', ') \
                                        FROM cfg.vw_raw_to_curated_column_mapping \
                                        WHERE config_id=" + str(config_id) + " AND active_ind=1 AND derived_ind=1 AND transformation_function_id IS NOT NULL")
        ret_source_query = ret_source_query_df.collect()[0][0]
    else:
        ret_source_query_df = spark.sql("SELECT 'SELECT ' + " + raw_columns + " FROM data_management.cfg.source_to_output_config WHERE config_id=" + str(config_id))
        ret_source_query = ret_source_query_df.collect()[0][0]

    # Get transformation function definitions
    cte_df = spark.sql("WITH CTE AS \
                        (SELECT custom_column_mapping_id, source_column_name, replace(transformation_function_def,'i0', source_column_name) as f_definition, transformation_parameter_text as f_parameter_text, transform_func_data_type as transform_data_type \
                         FROM cfg.vw_raw_to_curated_column_mapping \
                         WHERE active_ind=1 AND config_id=" + str(config_id) + " AND transformation_function_id IS NOT NULL) \
                        SELECT source_column_name, f_definition, f_parameter_text, transform_data_type \
                        FROM CTE \
                        ORDER BY custom_column_mapping_id")

    for row in cte_df.collect():
        column_name, f_def, p_list, p_dtype = row
        p_list = re.sub(r'[i](\d+)', r'c_name\1, c_pos\1', p_list)
        c_name_pos = re.findall(r'c_name(\d+), c_pos(\d+)', p_list)

        for c_name, c_pos in c_name_pos:
            f_def = f_def.replace('i' + c_pos, c_name)

        if p_dtype == 'Boolean':
            b_count += 1
            ret_bkeys += ';' + map_construct.format(x=column_name)
            ret_bvals += ';' + map_construct.format(x=f_def)
        elif p_dtype == 'Date':
            d_count += 1
            ret_dkeys += ';' + map_construct.format(x=column_name)
            ret_dvals += ';' + map_construct.format(x=f_def)
        elif p_dtype in ('Timestamp', 'DateTime'):
            dt_count += 1
            ret_dtkeys += ';' + map_construct.format(x=column_name)
            ret_dtvals += ';' + map_construct.format(x=f_def)
        elif p_dtype in ('Numeric', 'Decimal', 'Integer', 'Bit'):
            n_count += 1
            ret_nkeys += ';' + map_construct.format(x=column_name)
            ret_nvals += ';' + map_construct.format(x=f_def)
        elif p_dtype == 'String':
            s_count += 1
            ret_skeys += ';' + map_construct.format(x=column_name)
            ret_svals += ';' + map_construct.format(x=f_def)

    # Pad variables to return_length
    while b_count < return_length:
        b_count += 1
        ret_bkeys += ';"_b' + str(b_count) + '"'
        ret_bvals += ';"toBoolean(null())"'

    while d_count < return_length:
        d_count += 1
        ret_dkeys += ';"_d' + str(d_count) + '"'
        ret_dvals += ';"toDate(null())"'

    while dt_count < return_length:
        dt_count += 1
        ret_dtkeys += ';"_dt' + str(dt_count) + '"'
        ret_dtvals += ';"toTimestamp(null())"'

    while n_count < return_length:
        n_count += 1
        ret_nkeys += ';"_n' + str(n_count) + '"'
        ret_nvals += ';"toDecimal(null())"'

    while s_count < return_length:
        s_count += 1
        ret_skeys += ';"_s' + str(s_count) + '"'
        ret_svals += ';"toString(null())"'

    # Remove semi-colon from the beginning
    ret_bkeys = re.sub(r'^;', '', ret_bkeys)
    ret_bvals = re.sub(r'^;', '', ret_bvals)
    ret_dkeys = re.sub(r'^;', '', ret_dkeys)
    ret_dvals = re.sub(r'^;', '', ret_dvals)
    ret_dtkeys = re.sub(r'^;', '', ret_dtkeys)
    ret_dtvals = re.sub(r'^;', '', ret_dtvals)
    ret_nkeys = re.sub(r'^;', '', ret_nkeys)
    ret_nvals = re.sub(r'^;', '', ret_nvals)
    ret_skeys = re.sub(r'^;', '', ret_skeys)
    ret_svals = re.sub(r'^;', '', ret_svals)

    # Return variables with name changed
    return ret_source_query, ret_bkeys, ret_bvals, ret_dkeys, ret_dvals, ret_dtkeys, ret_dtvals, ret_nkeys, ret_nvals, ret_skeys, ret_svals
