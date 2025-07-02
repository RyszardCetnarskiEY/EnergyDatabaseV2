import json
import logging
logger = logging.getLogger(__name__)
import os
import sys
import random
from datetime import timedelta
from pendulum import datetime
from io import StringIO
import xml.etree.ElementTree as ET
import pandas as pd
import requests
from pendulum import datetime
from typing import Dict, Any, List, Union
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



ENTSOE_VARIABLES = {
    "Energy Prices Day Ahead Fixing I MAIN": {
        "table": "energy_prices_day_ahead_fixing_i_main",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:mRID', "resolution" : 'PT60M'},
        "params" : {"documentType": "A44"}
    },

    "Actual Total Load MAIN": {
        "table": "actual_total_load_main",
        "AreaType": "country_domain",
        'xml_parsing_info': {"column_name": 'ns:mRID', "resolution": 'PT15M'},
        "params": {"documentType": "A65", "processType": "A16"}
    },

    "Total Load Forecast Day Ahead MAIN": {
        "table": "total_load_forecast_day_ahead_main",
        "AreaType": "country_domain",
        'xml_parsing_info': {"column_name": 'ns:mRID', "resolution": 'PT15M'},
        "params": {"documentType": "A65", "processType": "A01"}
    },

    "Generation Forecasts for Wind and Solar MAIN": {
        "table": "generation_forecasts_for_wind_and_solar_main",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:MktPSRType/ns:psrType', "resolution" : 'PT15M'},
        "params" : {"documentType": "A69", "processType": "A01"}
    },

    "Actual Generation per Production Unit MAIN": {
        "table": "actual_generation_per_production_unit_main",
        "AreaType" : "bidding_zones",
        'xml_parsing_info' : {"column_name" : "ns:MktPSRType/ns:PowerSystemResources/ns:name",  "resolution" :'PT60M'},
        "params" : {"documentType": "A73", "processType": "A16"}
    },

    "Generation Forecasts Day Ahead MAIN": {
        "table": "generation_forecasts_day_ahead_main",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:mRID', "resolution" :'PT60M'},
        "params" : {"documentType": "A71", "processType": "A01"}
    }
}


POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"

COUNTRY_MAPPING = {
    "PL": {"name": "Poland", "country_domain": ["10YPL-AREA-----S"], "bidding_zones" : ["10YPL-AREA-----S"]},
    "DE": {"name": "Germany", "country_domain": ["10Y1001A1001A82H"], "bidding_zones": ["10YDE-VE-------2", "10YDE-EON------1", "10YDE-ENBW-----N", "10YDE-RWENET---I"]},
    "CZ": {"name": "Czech Republic", "country_domain": ["10YCZ-CEPS-----N"], "bidding_zones" : ["10YCZ-CEPS-----N"]},
}

HISTORICAL_START_DATE = datetime(2025, 6, 1, tz="UTC")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


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

def _generate_run_parameters_logic(data_interval_start, data_interval_end):
    """Core logic extracted for testing"""
    task_params = []
    
    for var_name, entsoe_params_dict in ENTSOE_VARIABLES.items():
        for country_code, country_details in COUNTRY_MAPPING.items():
            for in_domain in country_details[entsoe_params_dict["AreaType"]]:
                task_params.append({
                    "entsoe_api_params": {
                        "periodStart": data_interval_start.strftime('%Y%m%d%H%M'),
                        "periodEnd": data_interval_end.strftime('%Y%m%d%H%M'),
                        "in_Domain": in_domain,
                        **entsoe_params_dict["params"]
                    },
                    "task_run_metadata": {       
                        "var_name": var_name, 
                        "config_dict": entsoe_params_dict,
                        "country_code": country_code,
                        "country_name": country_details["name"],
                        "area_code": in_domain,
                    }
                })
    
    return task_params

@task
def generate_run_parameters(**context) -> List[Dict[str, str]]:
    data_interval_start = context['data_interval_start']
    data_interval_end = context['data_interval_end']
    
    task_params = _generate_run_parameters_logic(data_interval_start, data_interval_end)
    
    logger.info(f"Generated {len(task_params)} parameter sets for data interval: {data_interval_start.to_date_string()} - {data_interval_end.to_date_string()}")
    return task_params

def _entsoe_http_connection_setup():
    http_hook = HttpHook(method="GET", http_conn_id="ENTSOE")
    conn = http_hook.get_connection("ENTSOE")
    api_key = conn.password

    logger.info(f"securityToken: {api_key[0:10]!r}")  # the !r will show hidden chars
    # logger.info(f"securityToken: {api_key[10::]!r}")  # the !r will show hidden chars

    base_url = conn.host.rstrip("/")  # rstrip chyba nic nie robi, do usunięcia
    # Todo, do I need to define this connection anew every task instance?
    if conn.host.startswith("http"):
        base_url = conn.host.rstrip("/")
    else:
        base_url = (
            f"https://{conn.host.rstrip('/')}" + "/api"
        )  # Added because I think airflow UI connections and one defined in helm chart behave slightly differently

    logger.info(f"[DEBUG] REQUEST URL: {base_url}")

    return base_url, api_key, conn, http_hook

def _get_entsoe_response(log_str, api_request_params):
    logger.info(f"Fetching data for {log_str}")
    base_url, api_key, conn, http_hook = _entsoe_http_connection_setup()

    api_request_params = {
        "securityToken": api_key,
        **api_request_params,
    }
    session = http_hook.get_conn()
    response = session.get(base_url, params=api_request_params, timeout=60)

    response.raise_for_status()
    response.encoding = "utf-8"  # explicitly set encoding if not set
    return response

def get_domain_param_key(document_type: str, process_type: str) -> str | tuple:
    """
    Zwraca odpowiedni(e) parametr(y) domeny dla zapytania ENTSO-E API.
    """
    if document_type == "A65":  # Total Load (actual/forecast)
        return "outBiddingZone_Domain"
    
    elif document_type == "A44":  # Day-ahead prices
        return ("in_Domain", "out_Domain")
    
    elif document_type in ["A69", "A71", "A73", "A75"]:  # Generation Forecasts + Actual Generation per Unit
        return "in_Domain"
    
    return "in_Domain"

@task(task_id='extract_from_api')
def extract_from_api(task_param: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Fetches data from the ENTSOE API for a given country and period.
    api_params is expected to be a dict with 'periodStart', 'periodEnd', 'country_code'.
    """
    entsoe_api_params = task_param["entsoe_api_params"]
    task_run_metadata = task_param["task_run_metadata"]

    document_type = entsoe_api_params.get("documentType") #Poprawione
    process_type = entsoe_api_params.get("processType", "") #Poprawione
    domain_code = entsoe_api_params["in_Domain"]

    domain_param = get_domain_param_key(document_type, process_type)

    api_request_params = entsoe_api_params.copy() #Dodane
    if domain_param == "in_Domain":
        api_request_params["in_Domain"] = domain_code
    elif domain_param == "outBiddingZone_Domain":
        api_request_params["outBiddingZone_Domain"] = domain_code
    elif isinstance(domain_param, tuple):
        api_request_params["in_Domain"] = domain_code
        api_request_params["out_Domain"] = domain_code

    log_str = (
        f"{task_run_metadata['var_name']} {task_run_metadata['country_name']} "
        f"({entsoe_api_params['in_Domain']}) for period: {entsoe_api_params['periodStart']} - {entsoe_api_params['periodEnd']}"
    )

    try:
        response = _get_entsoe_response(log_str, api_request_params)
        logger.info(f"Successfully extracted data for  {log_str}")

        return {
            'xml_content': response.text,
            'status_code': response.status_code,
            'content_type': response.headers.get('Content-Type'),
            "var_name": task_run_metadata["var_name"],
            'country_name': task_run_metadata['country_name'],
            'country_code': task_run_metadata['country_code'],
            'area_code': domain_code,
            "period_start": entsoe_api_params["periodStart"],
            "period_end": entsoe_api_params["periodEnd"],
            'logical_date_processed': context['logical_date'].isoformat(),
            "request_params": json.dumps(api_request_params),
            'task_run_metadata': task_run_metadata
        }

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error for {log_str}: {http_err} - Response: {response.text}")
        raise AirflowException(f"API request failed for {log_str} with status {response.status_code}: {response.text}")
    except Exception as e:
        logger.error(f"Error extracting data for {log_str}: {str(e)}")
        raise

@task  
def store_raw_xml(extracted_data: Dict[str, Any], db_conn_id: str, table_name: str) -> int:
    if not extracted_data or 'xml_content' not in extracted_data:
        logger.warning(f"Skipping storage of raw XML due to missing content for params: {extracted_data.get('request_params')}")
        return -1
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    sql = f"""
    INSERT INTO airflow_data."{table_name}"
        (var_name, country_name, area_code, status_code, period_start, period_end, request_time, xml_data, request_parameters, content_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    RETURNING id;"""
    try:
        inserted_id = pg_hook.run(
            sql,
            parameters=(
                extracted_data['var_name'],
                extracted_data['country_name'],
                extracted_data['area_code'],
                extracted_data['status_code'],
                extracted_data['period_start'],
                extracted_data['period_end'],
                extracted_data['logical_date_processed'],
                extracted_data['xml_content'],
                extracted_data['request_params'],
                extracted_data['content_type'],
            ),
            handler=lambda cursor: cursor.fetchone()[0]
        )
        logger.info(f"Stored raw XML with ID {inserted_id} for request targeting {extracted_data.get('country_name')}")
        return inserted_id
    except Exception as e:
        logger.error(f"Error storing raw XML for {extracted_data.get('country_name')}: {e}")
        raise

@task(task_id='parse_xml')
def parse_xml(extracted_data: Dict[str, Any]) -> pd.DataFrame:
    xml_data = extracted_data['xml_content']
    country_name = extracted_data['country_name']
    area_code = extracted_data['area_code']

    var_name = extracted_data['var_name']
    column_name = extracted_data['task_run_metadata']['config_dict']["xml_parsing_info"]['column_name']
    var_resolution = extracted_data['task_run_metadata']['config_dict']["xml_parsing_info"]['resolution']
    request_params_str = extracted_data['request_params']

    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        raise ValueError(f"Invalid XML format: {e}")

    import xml.dom.minidom
    dom = xml.dom.minidom.parseString(xml_data)
    pretty_xml = dom.toprettyxml(indent="  ")

    ns = {'ns': root.tag[root.tag.find("{")+1:root.tag.find("}")]} if "{" in root.tag else {}
    print('ns: ', ns)

    max_pos = 0

    results_df = pd.DataFrame(columns = ["Position", "Period_Start", "Period_End", "Resolution", "quantity", "variable"])
    resolutions = [elem.text for elem in root.findall('.//ns:resolution', namespaces=ns)]
    found_res = var_resolution in resolutions
    for ts in root.findall('ns:TimeSeries', ns):
        name = ts.findtext(column_name, namespaces=ns)

        if var_name == "Actual Generation per Production Unit MAIN":
            name = ts.findtext('ns:MktPSRType/ns:PowerSystemResources/ns:mRID', namespaces=ns)
            registered_resource = ts.findtext('ns:registeredResource.mRID', namespaces=ns)
            ts_id = ts.findtext('ns:mRID', namespaces=ns)

            if not name: name = "no_name"
            if not registered_resource: registered_resource = "no_res"
            if not ts_id: ts_id = "no_ts_id"

            value_label = f"{var_name}__{registered_resource}__{ts_id}"
            ###value_label = registered_resource.strip()

        if not name:
            logger.warning(f"No mRID found for TimeSeries in {var_name} {country_name} ({area_code}) – skipping this block.")
            continue
        name = name.strip()

        period = ts.find('ns:Period', ns)
        if period is None:
            logger.error(f"No  data for {var_name} {country_name} {request_params_str}")
            continue
        resolution = period.findtext('.//ns:resolution', namespaces=ns)

        if resolution != var_resolution:
            if found_res:
                # W pliku XML są też TimeSeries z żądaną rozdzielczością — omiń inne
                continue
            else:
                # W XML nie ma TimeSeries z rozdzielczością z configa — przyjmij tę z XML
                logger.warning(f"Resolution mismatch for {var_name} {country_name} ({area_code}). Using XML resolution '{resolution}' instead of config '{var_resolution}'")
                var_resolution = resolution  # nadpisz dla bieżącego bloku


        timeInterval = period.findall('ns:timeInterval', ns)
        start = period.findtext('ns:timeInterval/ns:start', namespaces=ns)
        end = period.findtext('ns:timeInterval/ns:end', namespaces=ns)

        data = {}
        for point in period.findall('ns:Point', ns):
            pos = point.findtext('ns:position', namespaces=ns)
            qty = point.findtext('ns:quantity', namespaces=ns) or point.findtext('ns:price.amount', namespaces=ns) # TODO Also pass this in the xml parsing params like name
            if pos is not None and qty is not None:
                try:
                    pos = int(pos)
                    qty = float(qty)
                    data[pos] = qty
                    max_pos = max(max_pos, pos)
                except ValueError:
                    continue

        if var_name == "Generation Forecasts Day Ahead MAIN":
            ts_id = ts.findtext('ns:mRID', namespaces=ns)
            in_zone = ts.findtext('ns:inBiddingZone_Domain.mRID', namespaces=ns)
            out_zone = ts.findtext('ns:outBiddingZone_Domain.mRID', namespaces=ns)

            if in_zone:
                zone_type = "in"
            elif out_zone:
                zone_type = "out"
            else:
                zone_type = "unknown"

            value_label = f"{var_name}__{zone_type}"
            ###value_label = f"{var_name}"

        else:
            if column_name == 'ns:mRID':
                value_label = f"{var_name}__{area_code}"
                ###value_label = f"{var_name}"
            else:
                value_label = f"{name}__{area_code}"
                ###value_label = f"{var_name}"


        partial_df = pd.DataFrame.from_dict(data, orient='index', columns=["quantity"])
        partial_df.loc[:, "Period_Start"] = start
        partial_df.loc[:, "Period_End"] = end
        partial_df.loc[:, "Resolution"] = resolution
        partial_df.loc[:, "variable"] = value_label
        results_df = pd.concat([results_df, partial_df.reset_index(drop=False, names = 'Position' )], axis=0)

    if results_df.empty:
        logger.warning(f"Parsed XML with TimeSeries structure but no data rows extracted for {var_name} {country_name} {area_code}.")
        return pd.DataFrame()

    results_df["area_code"] = area_code


    logger.info(
        f"[parse_xml] Parsed {len(results_df)} rows for var_name={var_name}, "
        f"area_code={area_code}, table={extracted_data['task_run_metadata']['config_dict'].get('table')}"
    )

    return results_df

@task(task_id='add_timestamp')
def add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a timestamp column to the DataFrame based on Period_Start, Resolution, and index within TimeSeries_ID.
    Assumes Resolution is always PT60M (1 hour) for simplicity.
    """

    if df.empty:
        logger.info("Skipping timestamp addition for empty DataFrame.")
        return df.assign(timestamp=pd.NaT)

    df = df.copy()
    if 'Period_Start' not in df.columns or df['Period_Start'].isnull().all():
        logger.warning("Missing 'Period_Start' column or all values are null. Cannot add timestamp.")
        df['timestamp'] = pd.NaT
        return df.drop(columns=[col for col in ['Period_Start_dt', 'Resolution_td', 'Period_Start', 'Period_End'] if col in df.columns], errors='ignore')

    df['Period_Start_dt'] = pd.to_datetime(df['Period_Start'], utc=True, errors='coerce')

    def parse_resolution(res):
        if res == 'PT60M': return timedelta(hours=1)
        if res == 'PT30M': return timedelta(minutes=30)
        if res == 'PT15M': return timedelta(minutes=15)
        logger.warning(f"Unsupported resolution: {res}, will result in NaT timestamp for affected rows.")
        return pd.NaT

    df['Resolution_td'] = df['Resolution'].apply(parse_resolution)
    df['Position'] = pd.to_numeric(df['Position'], errors='coerce').fillna(0).astype(int)
    df['timestamp'] = df['Period_Start_dt'] + (df['Position'] - 1) * df['Resolution_td']
    df.drop(columns=['Period_Start_dt', 'Resolution_td', 'Period_Start', 'Period_End'], inplace=True, errors='ignore')
    return df

@task(task_id='add_timestamp_elements')
def add_timestamp_elements(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or 'timestamp' not in df.columns or df['timestamp'].isnull().all():
        logger.info("Skipping timestamp element addition for empty or invalid DataFrame.")

        for col in ['year', 'quarter', 'month', 'day', 'dayofweek', 'hour']:
            if col not in df.columns: df[col] = pd.NA 
        return df

    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    
    valid_timestamps = df['timestamp'].notna()
    df.loc[valid_timestamps, 'year'] = df.loc[valid_timestamps, 'timestamp'].dt.year.astype(int)
    df.loc[valid_timestamps, 'quarter'] = df.loc[valid_timestamps, 'timestamp'].dt.quarter.astype(int)
    df.loc[valid_timestamps, 'month'] = df.loc[valid_timestamps, 'timestamp'].dt.month.astype(int)
    df.loc[valid_timestamps, 'day'] = df.loc[valid_timestamps, 'timestamp'].dt.day.astype(int)
    df.loc[valid_timestamps, 'dayofweek'] = df.loc[valid_timestamps, 'timestamp'].dt.dayofweek.astype(int) # Monday=0, Sunday=6
    df.loc[valid_timestamps, 'hour'] = df.loc[valid_timestamps, 'timestamp'].dt.hour.astype(int)

    return df

@task
def zip_df_and_params(dfs: list, params: list) -> list[dict]:
    if len(dfs) != len(params):
        raise ValueError(f"Cannot zip: len(dfs)={len(dfs)} vs len(params)={len(params)}")
    return [{"df": df_obj, "task_param": param} for df_obj, param in zip(dfs, params)]

@task
def combine_df_and_params(df: pd.DataFrame, task_param: Dict[str, Any]):
    return {"df": df, "params": task_param}

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
def load_to_staging_table(df_and_params: Dict[str, Any], **context) -> Union[Dict[str, Any], str]:
    df = df_and_params['df']
    task_param = df_and_params['params']

    random_number = random.randint(0, 100000)
    if df.empty:
        logger.info(f"Skipping load to staging for {task_param['task_run_metadata']['country_name']} {task_param['entsoe_api_params']['periodStart']} as DataFrame is empty.")
        return f"empty_staging_{task_param['task_run_metadata']['country_code']}_{task_param['periodStart']}"

    df = df.drop("Position", axis=1)
    df['quantity'] = pd.to_numeric(df.loc[:, 'quantity'], errors='coerce').astype(float)
    cols = _create_table_columns(df)

    staging_table = f"stg_entsoe_{task_param['task_run_metadata']['country_code']}_{task_param['entsoe_api_params']['periodStart']}_{random_number}"

    staging_table = "".join(c if c.isalnum() else "_" for c in staging_table)

    staging_table = staging_table[:63]

    logger.info(
        f"Loading {len(df)} records to staging table: airflow_data.\"{staging_table}\" for {task_param['task_run_metadata']['country_name']}"
    )
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    create_stmt = f"""CREATE TABLE airflow_data."{staging_table}" (id SERIAL PRIMARY KEY, {", ".join(cols)}, processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);"""
    drop_stmt = f"""DROP TABLE IF EXISTS airflow_data."{staging_table}";"""
    pg_hook.run(drop_stmt)

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
    return {
        "staging_table_name": staging_table, 
        "var_name": task_param["task_run_metadata"]["var_name"],
    }

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

@task(task_id='merge_to_production_table')
def merge_data_to_production(staging_dict: Dict[str, Any], db_conn_id: str):
    staging_table_name = staging_dict['staging_table_name']
    production_table_name = staging_dict["var_name"].replace(" ", "_").lower()
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
        """
    
    try:
        pg_hook.run(merge_sql)
        logger.info(f"Successfully merged data from airflow_data.\"{staging_table_name}\" to airflow_data.\"{production_table_name}\".")
    except Exception as e:
        logger.error(f"Error merging data from {staging_table_name} to {production_table_name}: {e}")
        raise
    return f"Merged {staging_table_name}"

@task
def cleanup_staging_tables(staging_dict: Dict[str, Any], db_conn_id: str):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    table_name = staging_dict['staging_table_name']
    if table_name and not table_name.startswith("empty_staging_"):
        try:
            pg_hook.run(f'DROP TABLE IF EXISTS airflow_data."{table_name}";')
            logger.info(f"Dropped staging table: airflow_data.\"{table_name}\".")
        except Exception as e:
            logger.error(f"Error dropping staging table {table_name}: {e}")

@task
def log_etl_result(task_param: Dict[str, Any], db_conn_id: str, execution_date=None):

    entity = task_param["task_run_metadata"]["var_name"]
    country = task_param["task_run_metadata"]["country_name"]
    tso = task_param["task_run_metadata"]["area_code"]
    business_date = execution_date.format("YYYY-MM-DD")

    production_table_name = ENTSOE_VARIABLES[entity]["table"]

    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)

    try:
        sql = f"""
            SELECT COUNT(*) FROM airflow_data."{production_table_name}"
            WHERE date_trunc('day', "timestamp") = %s AND area_code = %s;
        """
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

    log_sql = """
    INSERT INTO airflow_data.entsoe_api_log (entity, country, tso, business_date, result, message)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (entity, country, tso, business_date)
    DO UPDATE SET result = EXCLUDED.result, message = EXCLUDED.message, logged_at = CURRENT_TIMESTAMP;
    """
    pg_hook.run(log_sql, parameters=(entity, country, tso, business_date, result, message))




@dag(
    dag_id='entsoe_new_etl_pipeline_final',
    default_args=default_args,
    description='Daily ETL for ENTSO-E day-ahead prices for multiple countries since 2023-01-01.',
    schedule='@daily',
    start_date=HISTORICAL_START_DATE,
    catchup=True,
    tags=['entsoe', 'energy', 'api', 'etl', 'new'],
    max_active_runs=1,
    doc_md=__doc__ 
)
def entsoe_new_etl_pipeline():

    log_table_created = create_log_table()

    initial_setup = create_initial_tables(
        db_conn_id=POSTGRES_CONN_ID,
        raw_xml_table=RAW_XML_TABLE_NAME
    )

    initial_setup.set_upstream(log_table_created)

    task_parameters = generate_run_parameters()
    task_parameters.set_upstream(initial_setup)

    extracted_data = extract_from_api.expand(task_param=task_parameters)

    stored_xml_ids = store_raw_xml.partial(
        db_conn_id=POSTGRES_CONN_ID,
        table_name=initial_setup["raw_xml_table"]
    ).expand(extracted_data=extracted_data)

    parsed_dfs = parse_xml.expand(extracted_data=extracted_data)
    parsed_dfs.set_upstream(stored_xml_ids)

    timestamped_dfs = add_timestamp_column.expand(df=parsed_dfs)

    enriched_dfs = add_timestamp_elements.expand(df=timestamped_dfs)

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
    
    log_result = log_etl_result.partial(db_conn_id=POSTGRES_CONN_ID).expand(task_param=task_parameters)

    log_result.set_upstream(merged_results)


#entsoe_new_etl_dag = entsoe_new_etl_pipeline()
