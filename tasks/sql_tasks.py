from datetime import timedelta
from pendulum import datetime
import json
import logging
import requests
import pandas as pd
from typing import Dict, Any, List
import xml.etree.ElementTree as ET
from io import StringIO
from typing import Any, Dict, Union
import random
import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)
POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing" # Changed name for clarity

from tasks.entsoe_dag_config import ENTSOE_VARIABLES

@task
def create_log_table():
    sql = """
    CREATE TABLE IF NOT EXISTS airflow_data.entsoe_api_log (
        id SERIAL PRIMARY KEY,
        entity TEXT,
        country TEXT,
        tso TEXT,
        business_date DATE,
        result TEXT CHECK (result IN ('success', 'fail')),
        message TEXT,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (entity, country, tso, business_date)
    );
    """
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql)

def _create_table_columns(df):
    create_table_columns = []
    for col_name, dtype in df.dtypes.items():
        pg_type = "TEXT"
        if "int" in str(dtype).lower():
            pg_type = "INTEGER"
        elif "float" in str(dtype).lower():
            pg_type = "NUMERIC"
        elif "datetime" in str(dtype).lower():
            pg_type = "TIMESTAMP WITH TIME ZONE"
        create_table_columns.append(f'"{col_name}" {pg_type}')
    return create_table_columns

@task(task_id='load_to_staging_table')
def load_to_staging_table(df_and_params: Dict[str, Any], db_conn_id: str, **context) -> Dict[str, Any]:
    try:
        df = df_and_params['df']
        task_param = df_and_params['task_param']

        if df.empty:
            message = f"Skipping load to staging for {task_param['task_run_metadata']['country_name']} {task_param['entsoe_api_params']['periodStart']} as DataFrame is empty."
            logger.info(message)
            return {
                "success": False,
                "staging_table_name": f"empty_staging_{task_param['task_run_metadata']['country_code']}_{task_param['entsoe_api_params']['periodStart']}",
                "var_name": task_param["task_run_metadata"]["var_name"],
                "task_param": task_param,
                "error": message
            }

        df = df.drop("Position", axis=1, errors="ignore")
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype(float)
        cols = _create_table_columns(df)

        random_number = random.randint(0, 100000)
        staging_table = f"stg_entsoe_{task_param['task_run_metadata']['country_code']}_{task_param['entsoe_api_params']['periodStart']}_{random_number}"
        staging_table = "".join(c if c.isalnum() else "_" for c in staging_table)[:63]

        logger.info(f"Loading {len(df)} records to staging table: airflow_data.\"{staging_table}\" for {task_param['task_run_metadata']['country_name']}")

        pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
        drop_stmt = f'DROP TABLE IF EXISTS airflow_data."{staging_table}";'
        create_stmt = f'CREATE TABLE airflow_data."{staging_table}" (id SERIAL PRIMARY KEY, {", ".join(cols)}, processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);'

        pg_hook.run(drop_stmt)
        pg_hook.run(create_stmt)

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, header=True)
        csv_buffer.seek(0)

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        sql_columns = ', '.join([f'"{col}"' for col in df.columns])

        try:
            cur.copy_expert(f'COPY airflow_data."{staging_table}" ({sql_columns}) FROM STDIN WITH CSV HEADER DELIMITER AS \',\' QUOTE \'\"\'', csv_buffer)

            conn.commit()
        finally:
            cur.close()
            conn.close()

        return {
            "success": True,
            "staging_table_name": staging_table,
            "var_name": task_param["task_run_metadata"]["var_name"],
            "task_param": task_param
        }

    except Exception as e:
        logger.error(f"[load_to_staging_table] Error: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "staging_table_name": "",
            "var_name": df_and_params['task_param']['task_run_metadata']['var_name'],
            "task_param": df_and_params['task_param']
        }

@task(task_id='merge_to_production_table')
def merge_data_to_production(staging_dict: Dict[str, Any], db_conn_id: str) -> Dict[str, Any]:
    try:
        staging_table_name = staging_dict.get('staging_table_name', '')
        task_param = staging_dict.get('task_param', {})
        var_name = staging_dict.get('var_name', '')

        production_table_name = var_name.replace(" ", "_").lower()

        if staging_table_name.startswith("empty_staging_") or not staging_dict.get("success", True):
            message = f"Skipping merge for empty/failed staging data: {staging_table_name}"
            logger.info(message)
            return {
                "success": False,
                "error": message,
                "staging_table_name": staging_table_name,
                "production_table_name": production_table_name,
                "task_param": task_param
            }

        pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
        _create_prod_table(production_table_name)

        merge_sql = f'''
            INSERT INTO airflow_data."{production_table_name}" (
                "timestamp", "resolution", "year", "quarter", "month", "day",
                dayofweek, hour, area_code, variable, quantity
            )
            SELECT 
                "timestamp", "Resolution", "year", "quarter", "month", "day",
                dayofweek, hour, area_code, variable, quantity
            FROM airflow_data."{staging_table_name}"
            WHERE timestamp IS NOT NULL AND variable IS NOT NULL
            ON CONFLICT (timestamp, variable, area_code) DO UPDATE SET
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
        '''

        pg_hook.run(merge_sql)
        logger.info(f"Successfully merged data from airflow_data.\"{staging_table_name}\" to airflow_data.\"{production_table_name}\".")
        return {
            "success": True,
            "staging_table_name": staging_table_name,
            "production_table_name": production_table_name,
            "task_param": task_param
        }

    except Exception as e:
        logger.error(f"[merge_data_to_production] Error: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "staging_table_name": staging_dict.get('staging_table_name', ''),
            "production_table_name": staging_dict.get('var_name', '').replace(" ", "_").lower(),
            "task_param": staging_dict.get('task_param', {})
        }

@task(task_id='create_initial_tables_if_not_exist')
def create_initial_tables(db_conn_id: str, raw_xml_table: str) -> dict:
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

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

    pg_hook.run(create_raw_sql)
    logger.info(f"Ensured raw XML table airflow_data.\"{raw_xml_table}\" exists.")
    
    return {"raw_xml_table": raw_xml_table}

def _create_prod_table(variable_name):

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
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
        UNIQUE (timestamp, variable, area_code)
        );
        """
        #UNIQUE (timestamp, variable)); TODO: Zmienione
    pg_hook.run(create_prod_sql)
    logger.info(f"Ensured production table airflow_data.\"{variable_name}\" exists.")

@task
def cleanup_staging_tables(staging_dict: Dict[str, Any], db_conn_id: str) -> Dict[str, Any]:
    try:
        pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
        table_name = staging_dict.get('staging_table_name', '')

        if table_name and not table_name.startswith("empty_staging_"):
            pg_hook.run(f'DROP TABLE IF EXISTS airflow_data."{table_name}";')
            logger.info(f"Dropped staging table: airflow_data.\"{table_name}\".")

        return {
            "success": True,
            "task_param": staging_dict.get("task_param", {}),
            "staging_table_name": table_name
        }

    except Exception as e:
        logger.error(f"[cleanup_staging_tables] Error: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "task_param": staging_dict.get("task_param", {}),
            "staging_table_name": staging_dict.get("staging_table_name", "")
        }

@task
def log_etl_result(merge_result: Dict[str, Any], db_conn_id: str, execution_date=None) -> Dict[str, Any]:
    task_param = merge_result.get("task_param", {})
    success = merge_result.get("success", False)
    error = merge_result.get("error", "")
    production_table_name = merge_result.get("production_table_name", "")

    entity = task_param.get("task_run_metadata", {}).get("var_name")
    country = task_param.get("task_run_metadata", {}).get("country_name")
    tso = task_param.get("task_run_metadata", {}).get("area_code")
    business_date = execution_date.format("YYYY-MM-DD") if execution_date else None

    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)

    if not success:
        result = "fail"
        message = error or f"Unknown error for {entity} on {business_date}"
    else:
        try:
            sql = f'''
                SELECT COUNT(*) FROM airflow_data."{production_table_name}"
                WHERE date_trunc('day', "timestamp") = %s AND area_code = %s;
            '''
            count = pg_hook.get_first(sql, parameters=(business_date, tso))[0]
            if count > 0:
                result = "success"
                message = f"Loaded {count} records to {production_table_name}"
            else:
                result = "fail"
                message = "No records found in production table"
        except Exception as e:
            result = "fail"
            message = f"Error checking production data: {str(e)}"

    log_sql = '''
    INSERT INTO airflow_data.entsoe_api_log (entity, country, tso, business_date, result, message)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (entity, country, tso, business_date)
    DO UPDATE SET result = EXCLUDED.result, message = EXCLUDED.message, logged_at = CURRENT_TIMESTAMP;
    '''
    try:
        pg_hook.run(log_sql, parameters=(entity, country, tso, business_date, result, message))
        return {"success": True, "result": result, "message": message, "task_param": task_param}
    except Exception as e:
        logger.error(f"[log_etl_result] Error logging to entsoe_api_log: {str(e)}")
        return {"success": False, "error": str(e), "task_param": task_param, "message": message}

@task
def filter_entities_to_run(task_params: list, db_conn_id: str, execution_date=None) -> list:
    pg = PostgresHook(postgres_conn_id=db_conn_id)
    date = execution_date.format("YYYY-MM-DD")

    filtered_params = []

    for param in task_params:
        entity = param["task_run_metadata"]["var_name"]
        country = param["task_run_metadata"]["country_name"]
        tso = param["task_run_metadata"]["area_code"]

        result = pg.get_first("""
            SELECT result, message FROM airflow_data.entsoe_api_log
            WHERE entity = %s AND country = %s AND tso = %s AND business_date = %s
            ORDER BY logged_at DESC LIMIT 1;
        """, parameters=(entity, country, tso, date))

        if not result:
            filtered_params.append(param)  # brak wpisu – trzeba zaciągnąć
        else:
            status, message = result
            if status == "fail":
                filtered_params.append(param)  # spróbujemy ponownie
            else:
                logging.info(f"Pomijam {entity} - {country} - {tso} na {date}, bo już pobrane: {message}")

    return filtered_params
