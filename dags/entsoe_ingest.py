import json
import sys
import os
import logging
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import debugpy
from datetime import timedelta
from pendulum import datetime
from typing import Dict, Any, List
from io import StringIO
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago

# Add the project root directory to sys.path so imports work from dags/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger = logging.getLogger(__name__)

from tasks.entsoe_dag_config import POSTGRES_CONN_ID, RAW_XML_TABLE_NAME, COUNTRY_MAPPING
from tasks.df_processing_tasks import add_timestamp_column, add_timestamp_elements, combine_df_and_params
from tasks.entsoe_api_tasks import generate_run_parameters, extract_from_api
from tasks.sql_tasks import load_to_staging_table, merge_data_to_production, create_initial_tables, cleanup_staging_tables_batch, create_log_table, log_etl_result, filter_entities_to_run
from tasks.xml_processing_tasks import store_raw_xml, parse_xml




#HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC") Do PYTEST
HISTORICAL_START_DATE = datetime(2023, 1, 1, tz="UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@task
def zip_df_and_params(dfs: list, task_params: list) -> list[dict]:
    try:
        if len(dfs) != len(task_params):
            raise ValueError(f"Cannot zip: len(dfs)={len(dfs)} vs len(task_params)={len(task_params)}")

        result = []
        for df_dict, param in zip(dfs, task_params):
            result.append({
                "df": df_dict.get("df", pd.DataFrame()),  # Teraz zawsze słownik z kluczem 'df'
                "task_param": param
            })
        return result
    except Exception as e:
        logger.error(f"[zip_df_and_params] Error: {str(e)}")
        return [{
            "success": False,
            "error": str(e),
            "task_param": task_params[0] if task_params else {},
            "df": pd.DataFrame()  # Zawsze zwracamy df
        }]

print('TODO - move to taskGroup one day and share some tasks for other variables, like generating units operation points')

@dag(
    dag_id='entsoe_dynamic_etl_pipeline_final',
    default_args=default_args,
    description='Daily ETL for ENTSO-E day-ahead prices for multiple countries since 2023-01-01.',
    schedule='@daily',
    start_date=HISTORICAL_START_DATE, # CRITICAL: Use timezone-aware datetime
    catchup=True,

    tags=["entsoe", "energy", "api", "etl", "dynamic"],
    max_active_runs=1,  # Limit to 1 active DAG run to avoid overwhelming API/DB during backfill
    doc_md=__doc__,

)
def entsoe_dynamic_etl_pipeline():

    log_table_created = create_log_table()

    initial_setup = create_initial_tables(
        db_conn_id=POSTGRES_CONN_ID,
        raw_xml_table=RAW_XML_TABLE_NAME
    )

    initial_setup.set_upstream(log_table_created)


    all_params = generate_run_parameters()
    all_params.set_upstream(initial_setup)

    task_parameters = filter_entities_to_run(
        task_params=all_params,
        db_conn_id=POSTGRES_CONN_ID
    )

    @task
    def extract_all(task_parameters: list) -> list:
        results = []
        for i, param in enumerate(task_parameters):
            logger.info("[extract_all] %d/%d var=%s area=%s",
                        i+1, len(task_parameters),
                        param["task_run_metadata"]["var_name"],
                        param["task_run_metadata"]["area_code"])
            res = extract_from_api.function(param)
            logger.info("[extract_all] result success=%s, keys=%s", res.get("success"), list(res.keys())[:10])
            results.append(res)
        logger.info("[extract_all] finished, total=%d", len(results))
        return results

    @task
    def store_all(extracted_data_list: list, db_conn_id: str, table_name: str) -> list:
        results = []
        for data in extracted_data_list:
            results.append(store_raw_xml.function(data, db_conn_id, table_name))
        return results

    @task
    def parse_all(extracted_data_list: list) -> list:
        results = []
        for data in extracted_data_list:
            results.append(parse_xml.function(data))
        return results

    # @task
    # def add_timestamp_all(parsed_dfs: list) -> list:
    #     results = []
    #     for df in parsed_dfs:
    #         # Serializacja DataFrame do JSON
    #         if isinstance(df, pd.DataFrame):
    #             results.append(df.to_json(orient="split"))
    #         elif isinstance(df, dict) and "df" in df and isinstance(df["df"], pd.DataFrame):
    #             results.append({**df, "df": df["df"].to_json(orient="split")})
    #         else:
    #             results.append(df)
    #     return results

    @task
    def add_timestamp_all(parsed_dfs: list) -> list:
        results = []
        for item in parsed_dfs:
            item_with_ts = add_timestamp_column.function(item)

            if isinstance(item_with_ts, dict) and "df" in item_with_ts and isinstance(item_with_ts["df"], pd.DataFrame):
                results.append({**item_with_ts, "df": item_with_ts["df"].to_json(orient="split")})
            else:
                results.append(item_with_ts)
        return results

    @task
    def add_timestamp_elements_all(parsed_dfs: list) -> list:
        results = []
        for df in parsed_dfs:
            # Deserializacja jeśli string JSON
            if isinstance(df, str):
                df = pd.read_json(df, orient="split")
            elif isinstance(df, dict) and "df" in df and isinstance(df["df"], str):
                df = {**df, "df": pd.read_json(df["df"], orient="split")}
            results.append(add_timestamp_elements.function(df))
        return results

    @task
    def zip_all(dfs: list, task_params: list) -> list:
        # Serializacja DataFrame w dict do JSON
        serializable = []
        for item in dfs:
            if isinstance(item, dict) and "df" in item and isinstance(item["df"], pd.DataFrame):
                serializable.append({**item, "df": item["df"].to_json(orient="split")})
            else:
                serializable.append(item)
        return zip_df_and_params.function(serializable, task_params)

    @task
    def combine_all(zipped: list) -> list:
        results = []
        for item in zipped:
            # Deserializacja DataFrame z JSON
            df = item["df"]
            if isinstance(df, str):
                df = pd.read_json(df, orient="split")
            results.append(combine_df_and_params.function(df, item["task_param"]))
        return results

    @task
    def load_staging_all(combined: list, db_conn_id: str) -> list:
        results = []
        for item in combined:
            results.append(load_to_staging_table.function(item))
        return results

    @task
    def merge_all(staging_dicts: list, db_conn_id: str) -> list:
        results = []
        for item in staging_dicts:
            results.append(merge_data_to_production.function(item, db_conn_id=db_conn_id))
        return results  # tu nie filtruj po success=True

    @task
    def cleanup_all(staging_dicts: list, db_conn_id: str):
        return cleanup_staging_tables_batch.function(staging_dicts, db_conn_id=db_conn_id)

    @task
    def log_all(merged_results: list, db_conn_id: str):
        results = []
        for item in merged_results:
            results.append(log_etl_result.function(item, db_conn_id=db_conn_id))
        return results



    extracted_data = extract_all(task_parameters)
    stored_xml_ids = store_all(extracted_data, POSTGRES_CONN_ID, initial_setup["raw_xml_table"])
    parsed_dfs = parse_all(extracted_data)
    parsed_dfs.set_upstream(stored_xml_ids)
    timestamped_dfs = add_timestamp_all(parsed_dfs)
    enriched_dfs = add_timestamp_elements_all(timestamped_dfs)
    zipped_args = zip_all(enriched_dfs, task_parameters)
    combined_for_staging = combine_all(zipped_args)
    staging_dict = load_staging_all(combined_for_staging, POSTGRES_CONN_ID)
    merged_results = merge_all(staging_dict, POSTGRES_CONN_ID)
    cleanup_task = cleanup_all(staging_dict, POSTGRES_CONN_ID)
    cleanup_task.set_upstream(merged_results)
    log_result = log_all(merged_results, POSTGRES_CONN_ID)
    log_result.set_upstream(merged_results)


entsoe_test_etl_dag = entsoe_dynamic_etl_pipeline()