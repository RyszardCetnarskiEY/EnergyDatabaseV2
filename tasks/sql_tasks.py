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

from airflow.operators.python import get_current_context

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

from airflow.operators.empty import EmptyOperator # Not used in final version, but good to know
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago # Alternative for start_date

import sys
import os

import logging

logger = logging.getLogger(__name__)
POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing" # Changed name for clarity



@task(task_id='load_to_staging_table')
def load_to_staging_table(df_and_params: Dict[str, Any], **context) -> str:

    df = df_and_params['df']
    task_param = df_and_params['params']

    if df.empty:
        logger.info(f"Skipping load to staging for {task_param['task_run_metadata']['country_name']} {task_param['entsoe_api_params']['periodStart']} as DataFrame is empty.")
        return f"empty_staging_{task_param['task_run_metadata']['country_code']}_{task_param['periodStart']}"

    df = df.drop("Position", axis=1)
    #id_vars = ["timestamp", "year", "quarter", "month", "day", "dayofweek", "hour", "area_code", "Resolution"]
    #df = pd.melt(df, id_vars = id_vars, value_name = 'quantity').copy()
    df['quantity'] = pd.to_numeric(df.loc[:, 'quantity'], errors='coerce').astype(float)
    
    dag_run_id_safe = context['dag_run'].run_id.replace(':', '_').replace('+', '_').replace('.', '_').replace('-', '_')
    staging_table = f"stg_entsoe_{task_param['task_run_metadata']['country_code']}_{task_param['entsoe_api_params']['periodStart']}_{dag_run_id_safe}"[:63] # Max PG table name length
    staging_table = "".join(c if c.isalnum() else "_" for c in staging_table) # Sanitize


    logger.info(f"Loading {len(df)} records to staging table: airflow_data.\"{staging_table}\" for {task_param['task_run_metadata']['country_name']}")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    create_table_columns = []
    for col_name, dtype in df.dtypes.items():
        pg_type = "TEXT" # Default
        if "int" in str(dtype).lower(): pg_type = "INTEGER"
        elif "float" in str(dtype).lower(): pg_type = "NUMERIC"
        elif "datetime" in str(dtype).lower(): pg_type = "TIMESTAMP WITH TIME ZONE"
        create_table_columns.append(f'"{col_name}" {pg_type}')

    create_stmt = f"""CREATE TABLE airflow_data."{staging_table}" (id SERIAL PRIMARY KEY, {', '.join(create_table_columns)}, processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);"""
    drop_stmt = f"""DROP TABLE IF EXISTS airflow_data."{staging_table}";"""
    pg_hook.run(drop_stmt) # Ensure clean slate for this run_id specific table
    pg_hook.run(create_stmt)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    csv_buffer.seek(0)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    sql_columns = ', '.join([f'"{col}"' for col in df.columns])
    try:
        cur.copy_expert(sql=f"""COPY airflow_data."{staging_table}" ({sql_columns}) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '"'""", file=csv_buffer)
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return {"staging_table_name": staging_table, "var_name":task_param["task_run_metadata"]["var_name"]}


@task(task_id='merge_to_production_table')
def merge_data_to_production(staging_dict: Dict[str, Any], db_conn_id: str):
    

    staging_table_name = staging_dict['staging_table_name']
    production_table_name = staging_dict["var_name"].replace(' ', '_').lower()
    if staging_table_name.startswith("empty_staging_"):
        logger.info(f"Skipping merge for empty/failed staging data: {staging_table_name}")
        return f"Skipped merge for {staging_table_name}"
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    _create_prod_table(production_table_name)

    merge_sql = f"""
        INSERT INTO airflow_data."{production_table_name}" (
            "timestamp", "resolution", "year", "quarter", "month", "day",
            dayofweek, hour, area_code, variable, quantity
        )
        SELECT 
            "timestamp", "Resolution", "year", "quarter", "month", "day",
            dayofweek, hour, area_code, variable, quantity
        FROM airflow_data."{staging_table_name}"
        WHERE timestamp IS NOT NULL AND variable IS NOT NULL
        ON CONFLICT (timestamp, variable) DO UPDATE SET
            "timestamp" = EXCLUDED."timestamp",
            "resolution" = EXCLUDED."resolution",
            year = EXCLUDED.year,
            quarter = EXCLUDED.quarter,
            month = EXCLUDED.month,
            day = EXCLUDED.day,
            dayofweek = EXCLUDED.dayofweek,
            hour = EXCLUDED.hour,
            area_code = EXCLUDED.area_code,
            variable = EXCLUDED.variable,
            quantity = EXCLUDED.quantity,
            processed_at = CURRENT_TIMESTAMP;
        """
    try:
        pg_hook.run(merge_sql)
        logger.info(f"Successfully merged data from airflow_data.\"{staging_table_name}\" to airflow_data.\"{production_table_name}\".")
    except Exception as e:
        logger.error(f"Error merging data from {staging_table_name} to {production_table_name}: {e}")
        raise
    return f"Merged {staging_table_name}"

    return 0


@task(task_id='create_initial_tables_if_not_exist')
def create_initial_tables(db_conn_id: str, raw_xml_table: str) -> dict:
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Raw XML Table
    create_raw_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{raw_xml_table}" (
        id SERIAL PRIMARY KEY, 
        var_name TEXT,
        country_name TEXT,
        area_code TEXT,
        status_code INTEGER,
        period_start TEXT,
        period_end TEXT,
        request_time TIMESTAMP WITH TIME ZONE, 
        xml_data XML, 
        request_parameters JSONB, 
        content_type TEXT);"""
    # Changed request_time to TIMESTAMP WITH TIME ZONE for consistency. xml_data allows NULL if extraction fails.
    pg_hook.run(create_raw_sql)
    logger.info(f"Ensured raw XML table airflow_data.\"{raw_xml_table}\" exists.")
    
    # debugpy.listen(("0.0.0.0", 8508))
    # debugpy.wait_for_client()
    # debugpy.breakpoint()
    return {"raw_xml_table": raw_xml_table}

def _create_prod_table(variable_name):

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    # Production Table
    create_prod_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{variable_name}" (
        id SERIAL PRIMARY KEY, 
        "timestamp" TIMESTAMP WITH TIME ZONE, 
        "resolution" TEXT,
        "year" INTEGER, 
        "quarter" INTEGER, 
        "month" INTEGER,
        "day" INTEGER, 
        "dayofweek" INTEGER, 
        "hour" INTEGER, 
        "area_code" TEXT, 
        "variable" TEXT, 
        "quantity" NUMERIC,
        processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (timestamp, variable));"""
    pg_hook.run(create_prod_sql)
    logger.info(f"Ensured production table airflow_data.\"{variable_name}\" exists.")




@task
def cleanup_staging_tables(staging_dict: Dict[str, Any], db_conn_id: str):

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    table_name = staging_dict['staging_table_name']
    if table_name and not table_name.startswith("empty_staging_"):
        try:
            pg_hook.run(f'DROP TABLE IF EXISTS airflow_data."{table_name}";')
            logger.info(f"Dropped staging table: airflow_data.\"{table_name}\".")
        except Exception as e:
            logger.error(f"Error dropping staging table {table_name}: {e}") # Log but don't fail DAG
