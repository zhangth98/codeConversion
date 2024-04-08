# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_transformation_func_mapping_arrays(config_id, return_length=8):
    spark = SparkSession.builder.getOrCreate()

    # Define the SQL query to get the raw columns
    raw_columns_query = """
    SELECT source_column_name
    FROM cfg.vw_raw_to_curated_column_mapping
    WHERE config_id={0} AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL))
    ORDER BY custom_column_mapping_id ASC
    """.format(config_id)
    
    raw_columns = ','.join([f"[{x}]" for x in spark.sql(raw_columns_query).select("source_column_name").distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()])

    # Define the SQL query to get the source query
    source_query_query = """
    SELECT source_column_name
    FROM cfg.vw_raw_to_curated_column_mapping
    WHERE config_id={0} AND active_ind=1 AND derived_ind=1 AND transformation_function_id IS NOT NULL
    ORDER BY custom_column_mapping_id ASC
    """.format(config_id)

    source_query=','.join([f"[{x}]" for x in spark.sql(source_query_query).select("source_column_name").distinct().orderBy("source_column_name").rdd.flatMap(lambda x: x).collect()])

    source_db_info=spark.sql('select source_database_name, source_schema_name,source_table_name from cfg.source_to_output_config where config_id=672').collect()[0]
    source_database_name, source_schema_name,source_table_name=source_db_info[0], source_db_info[1],source_db_info[2]

    if len(source_query) > 0:
        source_query = 'SELECT {0}, {1} FROM {2}.{3}.{4} WHERE AUDIT_ACTIVE_ROW_IND=''Y'''.format(
            raw_columns,
            source_query,
            source_database_name,
            source_schema_name,
            source_table_name
        )
    else:
        source_query = 'SELECT {0} FROM {1}.{2}.{3} WHERE AUDIT_ACTIVE_ROW_IND=''Y'''.format(
            raw_columns,
            source_database_name,
            source_schema_name,
            source_table_name
        )

    # Define the SQL query to get the function definitions
    function_definitions_query = """
    WITH CTE AS (
        SELECT custom_column_mapping_id, source_column_name,
               replace(transformation_function_def, 'i0', source_column_name) AS f_definition,
               transformation_parameter_text AS f_parameter_text, transform_func_data_type AS transform_data_type
        FROM cfg.vw_raw_to_curated_column_mapping
        WHERE active_ind=1 AND config_id={0} AND transformation_function_id IS NOT NULL
    )
    SELECT source_column_name, f_definition, f_parameter_text, transform_data_type
    FROM CTE
    ORDER BY custom_column_mapping_id;
    """.format(config_id)

    function_definitions = spark.sql(function_definitions_query)

    b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals = '', '', '', '', '', '', '', '', '', ''

    for row in function_definitions.collect():
        column_name, f_definition, f_parameter_text, transform_data_type = row

        # Replace the placeholders with column names
        for i, parameter in enumerate(f_parameter_text.split('|')):
            f_definition = f_definition.replace('i' + str(i + 1), parameter)

        # Add the function definition to the appropriate variable
        if transform_data_type == 'Boolean':
            b_keys += ';' + column_name
            b_vals += ';' + f_definition
        elif transform_data_type == 'Date':
            d_keys += ';' + column_name
            d_vals += ';' + f_definition
        elif transform_data_type in ('Timestamp', 'DateTime'):
            dt_keys += ';' + column_name
            dt_vals += ';' + f_definition
        elif transform_data_type in ('Numeric', 'Decimal', 'Integer', 'Bit'):
            n_keys += ';' + column_name
            n_vals += ';' + f_definition
        elif transform_data_type == 'String':
            s_keys += ';' + column_name
            s_vals += ';' + f_definition

    # Pad the variables to the specified length
    b_keys, b_vals = pad_variables(return_length,'b',b_keys, b_vals)
    d_keys, d_vals = pad_variables(return_length,'d',d_keys, d_vals)
    dt_keys, dt_vals = pad_variables(return_length,'dt',dt_keys, dt_vals)
    n_keys, n_vals = pad_variables(return_length,'n',n_keys, n_vals)
    s_keys, s_vals = pad_variables(return_length,'s',s_keys, s_vals)

    # Remove the semicolon from the beginning of each variable
    b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals = remove_semicolon(
        b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals
    )

    # Return the variables with renamed names
    return {
        'source_query': source_query,
        'b_keys': b_keys,
        'b_vals': b_vals,
        'd_keys': d_keys,
        'd_vals': d_vals,
        'dt_keys': dt_keys,
        'dt_vals': dt_vals,
        'n_keys': n_keys,
        'n_vals': n_vals,
        's_keys': s_keys,
        's_vals': s_vals
    }

def pad_variables(return_length, type_chars, keys, vals):
    l=len(keys.split(';')[1:])
    for j in range(l, return_length):
        keys += ';' + '_' + type_chars + str(j+1)
        if type_chars=='b':
            vals += ';' + 'toBoolean(null())'
        elif type_chars=='d':
             vals += ';' + 'toDate(null())'               
        elif type_chars=='dt':
             vals += ';' + 'toTimestamp(null())'    
        elif type_chars=='n':
             vals += ';' + 'toDecimal(null())'    
        elif type_chars=='s':
             vals += ';' + 'toString(null())'                                
    return (keys, vals)

def remove_semicolon(*args):
    results=[]
    for i, arg in enumerate(args):
        results.append(';'.join(arg.split(';')[1:]))

    return tuple(results)

# COMMAND ----------

get_transformation_func_mapping_arrays(672,40)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_transformation_func_mapping_arrays(config_id, return_length=8):
    spark = SparkSession.builder.getOrCreate()

    # Define the SQL query to get the raw columns
    raw_columns_query = """
    SELECT string_agg(concat('[', source_column_name, ']'), ',') WITHIN GROUP (ORDER BY custom_column_mapping_id)
    FROM cfg.vw_raw_to_curated_column_mapping
    WHERE config_id={0} AND active_ind=1 AND (derived_ind=0 OR (derived_ind=1 AND transformation_function_id IS NULL));
    """.format(config_id)

    raw_columns = spark.sql(raw_columns_query).collect()[0][0]

    # Define the SQL query to get the source query
    source_query_query = """
    SELECT string_agg(replace('cast(Null as varchar) as {x}', '{x}', source_column_name), ', ') WITHIN GROUP (ORDER BY custom_column_mapping_id)
    FROM cfg.vw_raw_to_curated_column_mapping
    WHERE config_id={0} AND active_ind=1 AND derived_ind=1 AND transformation_function_id IS NOT NULL;
    """.format(config_id)

    source_query = spark.sql(source_query_query).collect()[0][0]

    if len(source_query) > 0:
        source_query = 'SELECT {0}, {1} FROM {2}.{3}.{4} WHERE AUDIT_ACTIVE_ROW_IND=''Y'''.format(
            raw_columns,
            source_query,
            source_database_name,
            source_schema_name,
            source_table_name
        )
    else:
        source_query = 'SELECT {0} FROM {1}.{2}.{3} WHERE AUDIT_ACTIVE_ROW_IND=''Y'''.format(
            raw_columns,
            source_database_name,
            source_schema_name,
            source_table_name
        )

    # Define the SQL query to get the function definitions
    function_definitions_query = """
    WITH CTE AS (
        SELECT custom_column_mapping_id, source_column_name,
               replace(transformation_function_def, 'i0', source_column_name) AS f_definition,
               transformation_parameter_text AS f_parameter_text, transform_func_data_type AS transform_data_type
        FROM cfg.vw_raw_to_curated_column_mapping
        WHERE active_ind=1 AND config_id={0} AND transformation_function_id IS NOT NULL
    )
    SELECT source_column_name, f_definition, f_parameter_text, transform_data_type
    FROM CTE
    ORDER BY custom_column_mapping_id;
    """.format(config_id)

    function_definitions = spark.sql(function_definitions_query)

    b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals = '', '', '', '', '', '', '', '', '', ''

    for row in function_definitions.collect():
        column_name, f_definition, f_parameter_text, transform_data_type = row

        # Replace the placeholders with column names
        for i, parameter in enumerate(f_parameter_text.split('|')):
            f_definition = f_definition.replace('i' + str(i + 1), parameter)

        # Add the function definition to the appropriate variable
        if transform_data_type == 'Boolean':
            b_keys += ';' + column_name
            b_vals += ';' + f_definition
        elif transform_data_type == 'Date':
            d_keys += ';' + column_name
            d_vals += ';' + f_definition
        elif transform_data_type in ('Timestamp', 'DateTime'):
            dt_keys += ';' + column_name
            dt_vals += ';' + f_definition
        elif transform_data_type in ('Numeric', 'Decimal', 'Integer', 'Bit'):
            n_keys += ';' + column_name
            n_vals += ';' + f_definition
        elif transform_data_type == 'String':
            s_keys += ';' + column_name
            s_vals += ';' + f_definition

    # Pad the variables to the specified length
    b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals = pad_variables(
        return_length,
        b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals
    )

    # Remove the semicolon from the beginning of each variable
    b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals = remove_semicolon(
        b_keys, b_vals, d_keys, d_vals, dt_keys, dt_vals, n_keys, n_vals, s_keys, s_vals
    )

    # Return the variables with renamed names
    return {
        'source_query': source_query,
        'b_keys': b_keys,
        'b_vals': b_vals,
        'd_keys': d_keys,
        'd_vals': d_vals,
        'dt_keys': dt_keys,
        'dt_vals': dt_vals,
        'n_keys': n_keys,
        'n_vals': n_vals,
        's_keys': s_keys,
        's_vals': s_vals
    }

def pad_variables(return_length, *args):
    for i, arg in enumerate(args):
        while len(arg) < return_length:
            arg += ';' + '_' + arg[1:]

    return args

def remove_semicolon(*args):
    for i, arg in enumerate(args):
        arg = arg.strip(';')

    return args

# COMMAND ----------


