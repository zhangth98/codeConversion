# Databricks notebook source
# code tested on DBR 14.3LTS with openai, com.databricks:spark-xml_2.12:0.18.0 installed.
import os
from openai import OpenAI
import xml.etree.ElementTree as ET


def get_llm_response(
    source_code: str,
    prompt_text: str,
    system_prompt_text: str,
    model_name: str,
    api_key: str,
    api_base_url: str,
) -> str:

    client = OpenAI(base_url=api_base_url, api_key=api_key)

    chat_completion = client.chat.completions.create(
        messages=[
            {"role": "system", "content": f"{system_prompt_text}"},
            {"role": "user", "content": f"{prompt_text}:{source_code}"},
        ],
        model=model_name,
        max_tokens=4000,
    )

    return chat_completion.choices[0].message.content


def get_dts_executables(dts_file: str, dts_folder_path: str) -> list:

    with open(f"{dts_folder_path}/{dts_file}", "r") as file:
        package_content = file.read()

    # Parse the XML content
    root = ET.fromstring(package_content)

    # Find all "DTS:Executable" elements
    executables = root.findall(".//{www.microsoft.com/SqlServer/Dts}Executable")

    # Extract and convert each "DTS:Executable" to string
    executable_strings = []
    for executable in executables:
        executable_string = ET.tostring(executable, encoding="unicode", method="xml")
        executable_strings.append(executable_string)

    return executable_strings

# COMMAND ----------

def main(
    dts_file_name: str,
    model_name: str,
    prompt_text: str,
    system_prompt_text: str,
    save_result_to_table: bool = True,
    result_table: str = "codeconversionproject.default.ssis_conversion",
    ssis_folder_path: str = "",
    storage_format: str = "volume",
    secret_scope: str = "azz-wmpsandbox-secret-scope",
    api_secret_key: str = "dbrx-api-key",
    llm_api_endpoint_secret_key: str = "llm-api-endpoint",
) -> None:

    dts_folder_path = (
        f"{os.getcwd()}/{ssis_folder_path}"
        if storage_format == "workspace"
        else ssis_folder_path
    )
    executables = get_dts_executables(
        dts_file=dts_file_name, dts_folder_path=dts_folder_path
    )
    api_key = dbutils.secrets.get(secret_scope, api_secret_key)
    api_base_url = dbutils.secrets.get(secret_scope, llm_api_endpoint_secret_key)
    conversion_dl = []
    # Print or further process the executable strings
    for index, exe_string in enumerate(executables, start=1):
        response = get_llm_response(
            source_code=exe_string,
            prompt_text=prompt_text,
            system_prompt_text=system_prompt_text,
            model_name=model_name,
            api_key=api_key,
            api_base_url=api_base_url,
        )
        conversion_row = [
            "Databricks",
            model_name,
            ssis_folder_path,
            dts_file_name,
            system_prompt_text,
            prompt_text,
            exe_string,
            response,
        ]
        conversion_dl.append(conversion_row)

    conversion_cols = [
        "api_provider",
        "model_name",
        "ssis_file_path",
        "ssis_file_name",
        "system_prompt",
        "prompt_prefix",
        "ssis_component",
        "llm_response",
    ]

    df = spark.createDataFrame(conversion_dl).toDF(*conversion_cols)
    if save_result_to_table:
        df.write.format("delta").mode("append").saveAsTable(result_table)
    else:
        df.display()

# COMMAND ----------

if __name__ == "__main__":
    result_table = "codeconversionproject.default.ssis_conversion"
    save_result_to_table = False
    max_id = (
        spark.sql(f"select max(id) from {result_table}")
        .rdd.flatMap(lambda x: x)
        .collect()[0]
    )
    main(
        dts_file_name=dbutils.widgets.get("SSIS packge file"),
        model_name=dbutils.widgets.get("Model Name"),
        prompt_text="convert SSIS task to Databricks DLT workflow",
        system_prompt_text="""You are a Data Engineer. You want to convert a SSIS executable to a Databricks function in Pyspark. Keep in mind that the execution platform is Data Intelligence platfrom. Here are the tasks:
        1. understand the SSIS executable
        2. map all tables in SQL server to tables in Unity catalog
        3. Convert the SSIS executable to Databricks DLT workflow
        """,
        save_result_to_table=save_result_to_table,
        result_table="codeconversionproject.default.ssis_conversion",
        ssis_folder_path="SSIS",
        storage_format="workspace",
        secret_scope="azz-wmpsandbox-secret-scope",
        api_secret_key="dbrx-api-key",
        llm_api_endpoint_secret_key="llm-api-endpoint",
    )

    if not save_result_to_table:
        for response in (
            spark.sql(
                f"select llm_response from {result_table} where id>{max_id} order by id asc"
            )
            .rdd.flatMap(lambda x: x)
            .collect()
        ):
            print(response)

# COMMAND ----------

# %sql

# drop table codeconversionproject.default.ssis_conversion purge;

# create table codeconversionproject.default.ssis_conversion(
#   id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
#   api_provider string not null,
#   model_name string not null,
#   ssis_file_path string not null,
#   ssis_file_name string not null,
#   system_prompt string,
#   prompt_prefix string,
#   ssis_component string,
#   llm_response string
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC select llm_response
# MAGIC from codeconversionproject.default.ssis_conversion
# MAGIC where ssis_file_name='ODS - Employees.dtsx'
# MAGIC order by id asc