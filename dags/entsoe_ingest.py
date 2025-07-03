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

from tasks.entsoe_dag_config import POSTGRES_CONN_ID, RAW_XML_TABLE_NAME, COUNTRY_MAPPING
from tasks.df_processing_tasks import add_timestamp_column, add_timestamp_elements, combine_df_and_params
from tasks.entsoe_api_tasks import generate_run_parameters, extract_from_api
from tasks.sql_tasks import load_to_staging_table, merge_data_to_production, create_initial_tables, cleanup_staging_tables, create_log_table, log_etl_result, filter_entities_to_run
from tasks.xml_processing_tasks import store_raw_xml, parse_xml


#HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC") Do PYTEST
HISTORICAL_START_DATE = datetime(2025, 6, 15, tz="UTC")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def zip_df_and_params(dfs: list, params: list) -> list[dict]:
    try:
        if len(dfs) != len(params):
            raise ValueError(f"Cannot zip: len(dfs)={len(dfs)} vs len(params)={len(params)}")

        result = []
        for df_dict, param in zip(dfs, params):
            if isinstance(df_dict, dict):
                result.append({
                    "df": df_dict.get("df", pd.DataFrame()),
                    "task_param": param
                })
            else:
                result.append({
                    "df": df_dict,
                    "task_param": param
                })
        return result
    except Exception as e:
        logger.error(f"[zip_df_and_params] Error: {str(e)}")
        return [{
            "success": False,
            "error": str(e),
            "task_param": params[0] if params else {},
        }]

print('TODO - move to taskGroup one day and share some tasks for other variables, like generating units operation points')

@dag(
    dag_id='entsoe_dynamic_etl_pipeline_final',
    default_args=default_args,
    description='Daily ETL for ENTSO-E day-ahead prices for multiple countries since 2023-01-01.',
    schedule='@daily',
    start_date=HISTORICAL_START_DATE, # CRITICAL: Use timezone-aware datetime
    catchup=True,
    tags=['entsoe', 'energy', 'api', 'etl', 'dynamic'],
    max_active_runs=1, # Limit to 1 active DAG run to avoid overwhelming API/DB during backfill
    doc_md=__doc__ 
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

    extracted_data = extract_from_api.expand(task_param=task_parameters)

    stored_xml_ids = store_raw_xml.partial(
        db_conn_id=POSTGRES_CONN_ID,
        table_name=initial_setup["raw_xml_table"]
    ).expand(extracted_data=extracted_data)

    parsed_dfs = parse_xml.expand(extracted_data=extracted_data)
    parsed_dfs.set_upstream(stored_xml_ids)

    timestamped_dfs = add_timestamp_column.expand(parsed_data=parsed_dfs)

    enriched_dfs = add_timestamp_elements.expand(parsed_data=timestamped_dfs)

    zipped_args = zip_df_and_params(
        dfs=enriched_dfs,
        params=task_parameters,
    )

    combined_for_staging = combine_df_and_params.expand_kwargs(zipped_args)

    staging_dict = load_to_staging_table.partial(
        db_conn_id=POSTGRES_CONN_ID,
    ).expand(df_and_params=combined_for_staging)

    merged_results = merge_data_to_production.partial(
        db_conn_id=POSTGRES_CONN_ID
    ).expand(staging_dict=staging_dict)

    cleanup_task = cleanup_staging_tables.partial(
        db_conn_id=POSTGRES_CONN_ID
    ).expand(staging_dict=staging_dict)
    
    cleanup_task.set_upstream(merged_results)
    
    log_result = log_etl_result.partial(db_conn_id=POSTGRES_CONN_ID).expand(merge_result=merged_results)

    log_result.set_upstream(merged_results)


entsoe_dynamic_etl_dag = entsoe_dynamic_etl_pipeline()
