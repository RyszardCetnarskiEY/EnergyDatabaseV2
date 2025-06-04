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



# API_BASE_URL = "https://web-api.tp.entsoe.eu/api"

POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing" # Changed name for clarity

#https://www.entsoe.eu/data/energy-identification-codes-eic/eic-area-codes-map/

COUNTRY_MAPPING = {
    "PL": {"name": "Poland", "country_domain": ["10YPL-AREA-----S"], "bidding_zones" : ["10YPL-AREA-----S"]},
    #"DE": {"name": "Germany", "country_domain": ["10Y1001A1001A82H"], "bidding_zones": ["10YDE-VE-------2", "10YDE-EON------1", "10YDE-ENBW-----N"]},
    "DE": {"name": "Germany", "country_domain": ["10Y1001A1001A82H"], "bidding_zones": ["10YDE-VE-------2", "10YDE-EON------1", "10YDE-ENBW-----N", "10YDE-RWENET---I"]},
    #"LT": {"name": "Lithuania", "domain": "10YLT-1001A0008Q"},
    #"CZ": {"name": "Czech Republic", "country_domain": ["10YCZ-CEPS-----N"], "bidding_zones" : ["10YCZ-CEPS-----N"]},
    #"SK": {"name": "Slovakia", "domain": "10YSK-SEPS-----K"},
    #"SE": {"name": "Sweden", "domain": "10YSE-1--------K"},
    #"FR": {"name": "France", "domain": "10YFR-RTE------C"},
}
HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



def _generate_run_parameters_logic(data_interval_start, data_interval_end):
    """Core logic extracted for testing"""
    from entsoe_dag_config import ENTSOE_VARIABLES 
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

@task(task_id='extract_from_api')
def extract_from_api(task_param: Dict[str, Any],**context) -> str:
    """
    Fetches data from the ENTSOE API for a given country and period.
    api_params is expected to be a dict with 'periodStart', 'periodEnd', 'country_code'.
    """
    entsoe_api_params = task_param["entsoe_api_params"]
    task_run_metadata = task_param["task_run_metadata"]

    api_kwargs = {}
    if task_run_metadata["var_name"] == "Energy Prices fixing I":
        api_kwargs["out_Domain"] =  entsoe_api_params["in_Domain"]

    http_hook = HttpHook(method='GET', http_conn_id="ENTSOE")
    conn = http_hook.get_connection("ENTSOE")
    api_key = conn.password

    api_request_params = {
        "securityToken": api_key,
        **entsoe_api_params,
        **api_kwargs
    }
    base_url = conn.host.rstrip("/")  # e.g. https://transparency.entsoe.eu/api

    # Logging context
    log_str = (
        f"{task_run_metadata['var_name']} {task_run_metadata['country_name']} "
        f"({entsoe_api_params['in_Domain']}) for period: {entsoe_api_params['periodStart']} - {entsoe_api_params['periodEnd']}"
    )
    
    final_params = dict(api_request_params, **api_kwargs)
    try:
        logger.info(f"Fetching data for {log_str}")
        session = http_hook.get_conn()
        response = session.get(base_url, params=api_request_params, timeout=60)

        response.raise_for_status()
        logger.info(f"Successfully extracted data for  {log_str}")
        return {
            'xml_content': response.text,
            'xml_bytes': response.content, #TODO a bit heavy, choose one, content or text
            'status_code': response.status_code,
            'content_type': response.headers.get('Content-Type'),
            'var_name': task_run_metadata['var_name'],
            'country_name': task_run_metadata['country_name'],
            'country_code': task_run_metadata['country_code'], # Todo, trochę za dużo tych nazw dla kraju, może już kod niepotrzebny?
            'area_code': entsoe_api_params['in_Domain'],
            'period_start': entsoe_api_params['periodStart'],
            'period_end': entsoe_api_params['periodEnd'],
            'logical_date_processed': context['logical_date'].isoformat(), # or data_interval_start            
            'request_params': json.dumps(api_request_params),
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
    # debugpy.listen(("0.0.0.0", 8508))
    # debugpy.wait_for_client()
    # debugpy.breakpoint()
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

    xml_data = extracted_data['xml_bytes']
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

    # Pretty-print the XML
    import xml.dom.minidom
    dom = xml.dom.minidom.parseString(xml_data)
    pretty_xml = dom.toprettyxml(indent="  ")

    # Determine namespace
    ns = {'ns': root.tag[root.tag.find("{")+1:root.tag.find("}")]} if "{" in root.tag else {}
    print('ns: ', ns)

    max_pos = 0

    results_name_dict = {}

    resolutions = [elem.text for elem in root.findall('.//ns:resolution', namespaces=ns)]
    found_res = var_resolution in resolutions
    for ts in root.findall('ns:TimeSeries', ns):
        # Determine column name
        name = ts.findtext(column_name, namespaces=ns)
        if not name:
            raise ValueError(f"no data in xml for: {column_name}")
        name = name.strip()

        # Get the period and resolution (optional to use)
        #TODO - remove api key from request params string, best pass period start and end explicitly as dict, not a str resulting from json serialization - if possible in context od airflow Xcom
        period = ts.find('ns:Period', ns)
        if period is None:
            logger.error(f"No  data for {var_name} {country_name} {request_params_str}")
            continue
        #.// is for recursive search. Remeber, if putting more nested objects each  needs to begin with a namespace "ns:"
        resolution = period.findtext('.//ns:resolution', namespaces=ns)
        if resolution != var_resolution and found_res: 
            #logger.error(f"No {resolution} resolution data for {var_name} {country_name} {request_params_str}") #TODO: this is not an errr, sometimes there can be both 15 min and 60 min resolution data in the xml
            continue

        timeInterval = period.findall('ns:timeInterval', ns)
        start = period.findtext('ns:timeInterval/ns:start', namespaces=ns)
        end = period.findtext('ns:timeInterval/ns:end', namespaces=ns)

        # Extract all <Point> values
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
                    continue  # Skip malformed points
        
        if column_name == 'ns:mRID': 
            value_label = var_name
        else: 
            value_label = name

        if value_label not in results_name_dict:
            results_name_dict[value_label] = pd.DataFrame(columns=[value_label, "Period_Start", "Period_End"])
        
        partial_df = pd.DataFrame.from_dict(data, orient='index', columns=[value_label])
        #TODO: if constructing a multicolumn dataframe, there is no need to store period start and period end for each column. They will be exactly the same. Either drop duplicates or remove altogether
        partial_df.loc[:, "Period_Start"] = start
        partial_df.loc[:, "Period_End"] = end
        partial_df.loc[:, "Resolution"] = resolution
        results_name_dict[value_label] = pd.concat([results_name_dict[value_label], partial_df])

    if not results_name_dict:
        raise ValueError("No valid TimeSeries data found in XML.")

    df = pd.concat(results_name_dict, axis=1).T.drop_duplicates().T
    df.columns = df.columns.droplevel()
    df = df.reset_index(drop=False, names = 'Position' )
    #df.loc[:,"country_name"] = country_name
    df.loc[:,"area_code"] = area_code
    return df


@task(task_id='add_timestamp')
def add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a timestamp column to the DataFrame based on Period_Start, Resolution, and index within TimeSeries_ID.
    Assumes Resolution is always PT60M (1 hour) for simplicity.
    """
    if df.empty:
        logger.info("Skipping timestamp addition for empty DataFrame.")
        return df.assign(timestamp=pd.NaT) # Ensure column exists

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
        # Ensure columns exist even if empty
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

    # for col in ["year", "quarter", "month", "day", "dayofweek", "hour"]:
    #     df.loc[:,col] = int(df.loc[:,col])
    
    # # For rows where timestamp became NaT, fill with appropriate NA type
    # for col in ['year', 'quarter', 'month', 'day', 'dayofweek', 'hour']:
    #     if df[col].isnull().any(): # Check if any NA, then fill for consistency
    #         # Pandas uses pd.NA for integer NAs for some types
    #         if 'int' in str(df[col].dtype).lower() or 'float' in str(df[col].dtype).lower() :
    #             df[col] = df[col].astype('Int64') if 'int' in str(df[col].dtype).lower() else df[col].astype('Float64')
    #             df[col] = df[col].where(valid_timestamps, pd.NA)

    return df
@task
def combine_df_and_params(df: pd.DataFrame, task_param: Dict[str, Any]):
    return {"df": df, "params": task_param}

@task(task_id='load_to_staging_table')
def load_to_staging_table(df_and_params: Dict[str, Any], **context) -> str:
    df = df_and_params['df']
    task_param = df_and_params['params']

    if df.empty:
        logger.info(f"Skipping load to staging for {task_param['task_run_metadata']['country_name']} {task_param['entsoe_api_params']['periodStart']} as DataFrame is empty.")
        return f"empty_staging_{task_param['task_run_metadata']['country_code']}_{task_param['periodStart']}"

    df = df.drop("Position", axis=1)
    id_vars = ["timestamp", "year", "quarter", "month", "day", "dayofweek", "hour", "area_code", "Resolution"]
    df_melted = pd.melt(df, id_vars = id_vars, value_name = 'quantity').copy()
    df_melted['quantity'] = pd.to_numeric(df_melted.loc[:, 'quantity'], errors='coerce').astype(float)
    
    dag_run_id_safe = context['dag_run'].run_id.replace(':', '_').replace('+', '_').replace('.', '_').replace('-', '_')
    staging_table = f"stg_entsoe_{task_param['task_run_metadata']['country_code']}_{task_param['entsoe_api_params']['periodStart']}_{dag_run_id_safe}"[:63] # Max PG table name length
    staging_table = "".join(c if c.isalnum() else "_" for c in staging_table) # Sanitize


    logger.info(f"Loading {len(df_melted)} records to staging table: airflow_data.\"{staging_table}\" for {task_param['task_run_metadata']['country_name']}")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    create_table_columns = []
    for col_name, dtype in df_melted.dtypes.items():
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
    df_melted.to_csv(csv_buffer, index=False, header=True)
    csv_buffer.seek(0)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    sql_columns = ', '.join([f'"{col}"' for col in df_melted.columns])
    try:
        cur.copy_expert(sql=f"""COPY airflow_data."{staging_table}" ({sql_columns}) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '"'""", file=csv_buffer)
        conn.commit()
    finally:
        cur.close()
        conn.close()
    return {"staging_table_name": staging_table, "var_name":task_param["task_run_metadata"]["var_name"]}

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

