# Databricks notebook source
config_id=672
raw_columns_query = """
SELECT source_column_name
FROM cfg.vw_raw_to_curated_column_mapping
WHERE config_id={0} AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL))
ORDER BY custom_column_mapping_id ASC
""".format(config_id)

raw_columns = ','.join([f"[{x}]" for x in spark.sql(raw_columns_query).select("source_column_name").distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()])    

# Get the derived_columns value
derived_columns_query = """
SELECT source_column_name
FROM cfg.vw_raw_to_curated_column_mapping
WHERE config_id={0} AND active_ind=1 AND derived_ind=1 AND transformation_function_id IS NOT NULL
ORDER BY custom_column_mapping_id ASC
""".format(config_id)

derived_columns = ','.join([f"[{x}]" for x in spark.sql(derived_columns_query).select("source_column_name").distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()]) 

# COMMAND ----------

print(raw_columns)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_transformation_func_mapping_arrays(config_id, return_length=8):
    spark = SparkSession.builder.getOrCreate()

    # Define the required variables
    ret_source_query = ''
    raw_columns = ''
    map_construct = '"{x}"'
    ret_bkeys, ret_bvals, ret_dkeys, ret_dvals, ret_dtkeys, ret_dtvals, ret_nkeys, ret_nvals, ret_skeys, ret_svals = '', '', '', '', '', '', '', '', '', ''
    b_count, d_count, dt_count, n_count, s_count = 0, 0, 0, 0, 0

    # Get the raw_columns value
    raw_columns_query = """
    SELECT source_column_name
    FROM cfg.vw_raw_to_curated_column_mapping
    WHERE config_id={0} AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL))
    ORDER BY custom_column_mapping_id ASC
    """.format(config_id)
    
    raw_columns = [f"[{x}]" for x in spark.sql(raw_columns_query).select("source_column_name").distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()]

    # Get the derived_columns value
    derived_columns_query = """
    SELECT source_column_name
    FROM cfg.vw_raw_to_curated_column_mapping
    WHERE config_id={0} AND active_ind=1 AND derived_ind=1 AND transformation_function_id IS NOT NULL
    ORDER BY custom_column_mapping_id ASC
    """.format(config_id)
    
    derived_columns = [f"[{x}]" for x in spark.sql(derived_columns_query).select("source_column_name").distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()]

    source_columns=','.join(raw_columns + derived_columns)

    source_database_name,source_schema_name,source_table_name=spark.sql(f"""
            select source_database_name,source_schema_name,source_table_name
            from data_management.cfg.source_to_output_config
            where active_ind=1
            and config_id={config_id}""").collect()[0]
    # Get source query
    ret_source_query = f"SELECT {source_columns} FROM {source_database_name}.{source_schema_name}.{source_table_name} WHERE AUDIT_ACTIVE_ROW_IND='Y'"

    # Define the CTE query
    cte_query = f"WITH CTE AS ( \
                    SELECT custom_column_mapping_id, source_column_name, replace(transformation_function_def,'i0', source_column_name) as f_definition, transformation_parameter_text as f_parameter_text, transform_func_data_type as transform_data_type \
                    FROM cfg.vw_raw_to_curated_column_mapping \
                    WHERE active_ind=1 AND config_id={config_id} AND transformation_function_id IS NOT NULL \
                ) \
                SELECT source_column_name, f_definition, f_parameter_text, transform_data_type \
                FROM CTE \
                ORDER BY custom_column_mapping_id"

    # Execute the CTE query and iterate through the results
    for row in spark.sql(cte_query).collect():
        column_name, f_def, p_list, p_dtype = row

        # Replace the placeholders with column names
        for i, c_name in enumerate(p_list.split('|')):
            f_def = f_def.replace(f'i{i + 1}', c_name)

        # Assign the function definition to the appropriate variable based on the data type
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

    # Pad each variable to the specified return_length
    for i in range(return_length - b_count):
        ret_bkeys += ';"_b' + str(i + b_count + 1) + '"'
        ret_bvals += ';"toBoolean(null())"'

    for i in range(return_length - d_count):
        ret_dkeys += ';"_d' + str(i + d_count + 1) + '"'
        ret_dvals += ';"toDate(null())"'

    for i in range(return_length - dt_count):
        ret_dtkeys += ';"_dt' + str(i + dt_count + 1) + '"'
        ret_dtvals += ';"toTimestamp(null())"'

    for i in range(return_length - n_count):
        ret_nkeys += ';"_n' + str(i + n_count + 1) + '"'
        ret_nvals += ';"toDecimal(null())"'

    for i in range(return_length - s_count):
        ret_skeys += ';"_s' + str(i + s_count + 1) + '"'
        ret_svals += ';"toString(null())"'

    # Remove the semi-colon from the beginning of each variable
    ret_bkeys = ret_bkeys[1:]
    ret_bvals = ret_bvals[1:]
    ret_dkeys = ret_dkeys[1:]
    ret_dvals = ret_dvals[1:]
    ret_dtkeys = ret_dtkeys[1:]
    ret_dtvals = ret_dtvals[1:]
    ret_nkeys = ret_nkeys[1:]
    ret_nvals = ret_nvals[1:]
    ret_skeys = ret_skeys[1:]
    ret_svals = ret_svals[1:]

    # Return the results
    return {
        'source_query': ret_source_query,
        'bkeys': ret_bkeys,
        'bvals': ret_bvals,
        'dkeys': ret_dkeys,
        'dvals': ret_dvals,
        'dtkeys': ret_dtkeys,
        'dtvals': ret_dtvals,
        'nkeys': ret_nkeys,
        'nvals': ret_nvals,
        'skeys': ret_skeys,
        'svals': ret_svals
    }

# COMMAND ----------

get_transformation_func_mapping_arrays(672, 40)


# COMMAND ----------


