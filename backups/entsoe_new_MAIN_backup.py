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

HISTORICAL_START_DATE = datetime(2025, 6, 15, tz="UTC")

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

    document_type = entsoe_api_params.get("documentType")
    process_type = entsoe_api_params.get("processType", "")
    domain_code = entsoe_api_params["in_Domain"]

    domain_param = get_domain_param_key(document_type, process_type)

    api_request_params = entsoe_api_params.copy()
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
        return {
            'success': True,
            'xml_content': response.text,
            'status_code': response.status_code,
            'content_type': response.headers.get('Content-Type'),
            'var_name': task_run_metadata['var_name'],
            'country_name': task_run_metadata['country_name'],
            'country_code': task_run_metadata['country_code'],
            'area_code': domain_code,
            "period_start": entsoe_api_params["periodStart"],
            "period_end": entsoe_api_params["periodEnd"],
            'logical_date_processed': context['logical_date'].isoformat(),
            'request_params': json.dumps(api_request_params),
            'task_run_metadata': task_run_metadata
        }
    except Exception as e:
        logger.error(f"[extract_from_api] {log_str}: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "task_param": task_param
        }

@task(task_id='store_raw_xml')
def store_raw_xml(extracted_data: Dict[str, Any], db_conn_id: str, table_name: str) -> Dict[str, Any]:
    try:
        if not extracted_data.get("success", False):
            return {**extracted_data, "stored": False, "error": extracted_data.get("error", "extract failed")}

        pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
        sql = f"""
        INSERT INTO airflow_data."{table_name}"
            (var_name, country_name, area_code, status_code, period_start, period_end, request_time, xml_data, request_parameters, content_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        RETURNING id;"""

        inserted_id = pg_hook.run(
            sql,
            parameters=(
                extracted_data['task_param']['task_run_metadata']['var_name'],
                extracted_data['task_param']['task_run_metadata']['country_name'],
                extracted_data['task_param']['task_run_metadata']['area_code'],
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
        return {**extracted_data, "stored": True, "raw_id": inserted_id}
    except Exception as e:
        logger.error(f"[store_raw_xml] Error: {str(e)}")
        return {**extracted_data, "success": False, "stored": False, "error": str(e)}

@task(task_id='parse_xml')
def parse_xml(extracted_data: Dict[str, Any]) -> pd.DataFrame:
    try:
        if not extracted_data.get("success", False):
            return {**extracted_data, "success": False, "error": "Upstream failure in extract_from_api"}

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

        #return results_df
        return {**extracted_data, "success": True, "df": results_df}
    
    except Exception as e:
        logger.error(f"[parse_xml] Error: {str(e)}")
        return {**extracted_data, "success": False, "error": str(e), "df": pd.DataFrame()}

@task(task_id='add_timestamp')
def add_timestamp_column(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not parsed_data.get("success", False):
            return {**parsed_data, "success": False, "error": parsed_data.get("error", "Upstream error")}

        df = parsed_data.get("df", pd.DataFrame())
        if df.empty:
            return {**parsed_data, "success": False, "error": "Empty DataFrame after parse_xml"}

        df = df.copy()
        df['Period_Start_dt'] = pd.to_datetime(df['Period_Start'], utc=True, errors='coerce')

        def parse_resolution(res):
            if res == 'PT60M': return timedelta(hours=1)
            if res == 'PT30M': return timedelta(minutes=30)
            if res == 'PT15M': return timedelta(minutes=15)
            return pd.NaT

        df['Resolution_td'] = df['Resolution'].apply(parse_resolution)
        df['Position'] = pd.to_numeric(df['Position'], errors='coerce').fillna(0).astype(int)
        df['timestamp'] = df['Period_Start_dt'] + (df['Position'] - 1) * df['Resolution_td']
        df.drop(columns=['Period_Start_dt', 'Resolution_td', 'Period_Start', 'Period_End'], inplace=True, errors='ignore')

        return {**parsed_data, "df": df, "success": True}
    except Exception as e:
        logger.error(f"[add_timestamp_column] Error: {str(e)}")
        return {**parsed_data, "success": False, "error": str(e)}

@task(task_id='add_timestamp_elements')
def add_timestamp_elements(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not parsed_data.get("success", False):
            return {**parsed_data, "success": False}

        df = parsed_data.get("df", pd.DataFrame())
        if df.empty or "timestamp" not in df.columns:
            return {**parsed_data, "success": False, "error": "Missing timestamp"}

        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        valid = df['timestamp'].notna()

        df.loc[valid, 'year'] = df.loc[valid, 'timestamp'].dt.year.astype(int)
        df.loc[valid, 'quarter'] = df.loc[valid, 'timestamp'].dt.quarter.astype(int)
        df.loc[valid, 'month'] = df.loc[valid, 'timestamp'].dt.month.astype(int)
        df.loc[valid, 'day'] = df.loc[valid, 'timestamp'].dt.day.astype(int)
        df.loc[valid, 'dayofweek'] = df.loc[valid, 'timestamp'].dt.dayofweek.astype(int)
        df.loc[valid, 'hour'] = df.loc[valid, 'timestamp'].dt.hour.astype(int)

        return {**parsed_data, "df": df, "success": True}
    except Exception as e:
        logger.error(f"[add_timestamp_elements] Error: {str(e)}")
        return {**parsed_data, "success": False, "error": str(e)}

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

@task
def combine_df_and_params(df: pd.DataFrame, task_param: Dict[str, Any]) -> Dict[str, Any]:
    try:
        #return {"df": df, "params": task_param, "success": not df.empty}
        return {"df": df, "task_param": task_param, "success": not df.empty}
    except Exception as e:
        logger.error(f"[combine_df_and_params] Error: {str(e)}")
        #return {"df": pd.DataFrame(), "params": task_param, "success": False, "error": str(e)}
        return {"df": pd.DataFrame(), "task_param": task_param, "success": False, "error": str(e)}

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
        #task_param = df_and_params['params']
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
            #cur.copy_expert(sql=f'COPY airflow_data."{staging_table}" ({sql_columns}) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE """"', file=csv_buffer)
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
            #"var_name": df_and_params['params']['task_run_metadata']['var_name'],
            #"task_param": df_and_params['params']
            "var_name": df_and_params['task_param']['task_run_metadata']['var_name'],
            "task_param": df_and_params['task_param']
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

    #timestamped_dfs = add_timestamp_column.expand(df=parsed_dfs)
    timestamped_dfs = add_timestamp_column.expand(parsed_data=parsed_dfs)

    #enriched_dfs = add_timestamp_elements.expand(df=timestamped_dfs)
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


#entsoe_new_etl_dag = entsoe_new_etl_pipeline()
