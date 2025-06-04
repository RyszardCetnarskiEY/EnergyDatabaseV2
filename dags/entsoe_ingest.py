from datetime import timedelta
from pendulum import datetime
import json
import logging
import requests
import pandas as pd
from typing import Dict, Any, List
import xml.etree.ElementTree as ET
from io import StringIO
import debugpy


from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

from airflow.operators.empty import EmptyOperator # Not used in final version, but good to know
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago # Alternative for start_date

import sys
import os

# Add the project root directory to sys.path so imports work from dags/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from entsoe_dag_config import POSTGRES_CONN_ID, RAW_XML_TABLE_NAME, COUNTRY_MAPPING
from tasks.df_processing_tasks import add_timestamp_column, add_timestamp_elements, combine_df_and_params
from tasks.entsoe_api_tasks import generate_run_parameters, extract_from_api
from tasks.sql_tasks import load_to_staging_table, merge_data_to_production, create_initial_tables, cleanup_staging_tables
from tasks.xml_processing_tasks import store_raw_xml, parse_xml


HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")

#logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}




print('TODO - move to taskGroup one day and share some tasks for other variables, like generating units operation points')
# A more advanced pattern could be a TaskGroup that is mapped.

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
    # --- Task Orchestration ---

    initial_setup = create_initial_tables(
        db_conn_id=POSTGRES_CONN_ID,
        raw_xml_table=RAW_XML_TABLE_NAME
    )

    task_parameters = generate_run_parameters()

    # Ensure setup is done before dynamic tasks
    task_parameters.set_upstream(initial_setup)

    extracted_data = extract_from_api.expand(task_param=task_parameters)

    stored_xml_ids = store_raw_xml.partial(
        db_conn_id=POSTGRES_CONN_ID,
        table_name=initial_setup["raw_xml_table"] # Use XComArg from setup task
    ).expand(extracted_data=extracted_data)

    parsed_dfs = parse_xml.expand(extracted_data=extracted_data)

    timestamped_dfs = add_timestamp_column.expand(df=parsed_dfs)

    enriched_dfs = add_timestamp_elements.expand(df=timestamped_dfs)

    combined_for_staging = combine_df_and_params.expand(df=enriched_dfs, task_param=task_parameters)

    staging_dict = load_to_staging_table.partial(
        db_conn_id=POSTGRES_CONN_ID
    ).expand(df_and_params=combined_for_staging)

    merged_results = merge_data_to_production.partial(
        db_conn_id=POSTGRES_CONN_ID
    ).expand(staging_dict=staging_dict)

    cleanup_task = cleanup_staging_tables.partial(
        db_conn_id=POSTGRES_CONN_ID
    ).expand(staging_dict=staging_dict)
    
    #Set cleanup to run after all merges
    cleanup_task.set_upstream(merged_results)
    
    # Explicitly make sure XML storage is upstream of parsing (though usually inferred)
    parsed_dfs.set_upstream(stored_xml_ids)



entsoe_dynamic_etl_dag = entsoe_dynamic_etl_pipeline()

