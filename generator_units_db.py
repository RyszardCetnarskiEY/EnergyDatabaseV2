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
from entsoe_dag_config import POSTGRES_CONN_ID, RAW_XML_TABLE_NAME, COUNTRY_MAPPING
from entsoe_ingest import extract_from_api
import sys
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")



task_param = {
'entsoe_api_params':{
    'BiddingZone_Domain': '10YPL-AREA-----S',
    'documentType': 'A95',
    'businessType':'B11',
    'Implementation_DateAndOrTime': '2025-01-01',


},
'task_run_metadata':{
    'var_name' : 'Production and Generation Units' 
}

}

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
    pass