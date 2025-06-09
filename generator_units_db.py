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

from tasks.entsoe_api_tasks import extract_from_api

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")



pl_task_param = {
'entsoe_api_params':{
    'BiddingZone_Domain': '10YPL-AREA-----S',
    'in_Domain': '10YPL-AREA-----S', # not used, only for logger compatibillity 
    'documentType': 'A95',
    'businessType':'B11',
    'Implementation_DateAndOrTime': '2025-01-01',


},
'task_run_metadata':{
    'var_name' : 'Production and Generation Units',
    "country_code": "10YPL-AREA-----S",
    "country_name": "Poland",
}

}

@dag(
    dag_id='entsoe_generators_db',
    default_args=default_args,
    description='Yearly ETL for ENTSO-E generator units database',
    schedule='@yearly',
    start_date=HISTORICAL_START_DATE, # CRITICAL: Use timezone-aware datetime
    catchup=False,
    tags=['entsoe', 'energy', 'api', 'etl', 'generators', 'yearly'],
    max_active_runs=1, # Limit to 1 active DAG run to avoid overwhelming API/DB during backfill
    doc_md=__doc__ 
)
def entsoe_gen_db_pipeline():
    extracted_data = extract_from_api(task_param=pl_task_param)
    # xml_data = extracted_data['xml_content']
    #     # Pretty-print the XML
    # import xml.dom.minidom
    # dom = xml.dom.minidom.parseString(xml_data)
    # pretty_xml = dom.toprettyxml(indent="  ")
    

entsoe_gen_db_dag = entsoe_gen_db_pipeline()
