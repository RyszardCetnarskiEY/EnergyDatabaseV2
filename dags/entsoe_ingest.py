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
from airflow.operators.empty import EmptyOperator # Not used in final version, but good to know
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago # Alternative for start_date

import sys
import os

# Add the project root directory to sys.path so imports work from dags/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Best practice: Store API keys in Airflow Connections or a secrets backend
ENTSOE_API_KEY = '43a243bd-0370-40c2-9b15-ccbf61c103b1' # Replace with Airflow Variable or Connection
API_BASE_URL = "https://web-api.tp.entsoe.eu/api"

POSTGRES_CONN_ID = "postgres_default"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing" # Changed name for clarity
PRODUCTION_TABLE_NAME = "entsoe_day_ahead_prices_prod" # Changed name for clarity
# samo DE: "10Y1001A1001A83F"
# DE-LU: "10Y1001A1001A82H"
# Wyrzucona jedna strefa DE "10Y1001C--00002H"
#https://www.entsoe.eu/data/energy-identification-codes-eic/eic-area-codes-map/
# To działa: "10YDE-VE-------2"
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

    @task
    def generate_run_parameters(**context)-> List[Dict[str, str]]:

        data_interval_start = context['data_interval_start']
        data_interval_end = context['data_interval_end']
        from entsoe_dag_config import ENTSOE_VARIABLES 
        task_params = []
        for var_name, entsoe_params_dict in ENTSOE_VARIABLES.items():

            for country_code, country_details in COUNTRY_MAPPING.items():
                for zone in country_details[entsoe_params_dict["AreaType"]]:
                    task_params.append(
                        {
                        "entsoe_api_params":
                            {
                            "periodStart": data_interval_start.strftime('%Y%m%d%H%M'),
                            "periodEnd": data_interval_end.strftime('%Y%m%d%H%M'),
                            "in_Domain": zone,
                            **entsoe_params_dict["params"]
                            },
                        
                        "task_run_metadata" : 
                            {       
                            "var_name" : var_name, 
                            "config_dict" : entsoe_params_dict,
                            "country_code": country_code,
                            "country_name": country_details["name"],
                            }
                        },
                    )

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
        if task_run_metadata["var_name"] == "Energy Prices":
            api_kwargs["out_Domain"] =  entsoe_api_params["in_Domain"]

        api_request_params = {
            "securityToken": ENTSOE_API_KEY,
            **entsoe_api_params,
        }
        
        final_params = dict(api_request_params, **api_kwargs)
        try:
            log_str = f"Fetching data for {task_run_metadata['var_name']} {task_run_metadata['country_name']} ({entsoe_api_params['in_Domain']}) for period: {entsoe_api_params['periodStart']}-{entsoe_api_params['periodEnd']}"
            logger.info(f"Fetching data for {log_str}")
            response = requests.get(API_BASE_URL, params=final_params, timeout=60)
            response.raise_for_status()
            logger.info(f"Successfully extracted data for  {log_str}")
            return {
                'xml_content': response.text,
                'status_code': response.status_code,
                'content_type': response.headers.get('Content-Type'),
                'request_params': json.dumps(api_request_params),
                'logical_date_processed': context['logical_date'].isoformat(), # or data_interval_start
                'country_name': task_run_metadata['country_name'],
                'country_code': task_run_metadata['country_code'],
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
            (xml_data, request_time, request_parameters, status_code, content_type)
        VALUES (%s, %s, %s::jsonb, %s, %s)
        RETURNING id;"""
        try:
            inserted_id = pg_hook.run(
                sql,
                parameters=(
                    extracted_data['xml_content'],
                    extracted_data['logical_date_processed'],
                    extracted_data['request_params'],
                    extracted_data['status_code'],
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
        # data_interval_start is timezone-aware if the DAG start_date is.
        debugpy.listen(("0.0.0.0", 8509))  # Replace port if needed
        debugpy.wait_for_client()

        xml_data = extracted_data['xml_content']
        country_name = extracted_data['country_name']
        var_name = extracted_data['task_run_metadata']['var_name']
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
        print(pretty_xml)

        # Determine namespace
        ns = {'ns': root.tag[root.tag.find("{")+1:root.tag.find("}")]} if "{" in root.tag else {}
        #ns = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}
        print('ns: ', ns)
        # Prepare a dictionary to collect all series
        series_dict = {}
        max_pos = 0

        results_name_dict = {}
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
            if resolution != var_resolution: #TODO, co jeżeli wcześniejszy okres będzie miał resolution PT60M a potem się zmieni, zrób kod odporny na taki warunek
                logger.error(f"No {var_resolution} resolution data for {var_name} {country_name} {request_params_str}")
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
            results_name_dict[value_label] = pd.concat([results_name_dict[value_label], partial_df])

        if not results_name_dict:
            raise ValueError("No valid TimeSeries data found in XML.")

        df = pd.concat(results_name_dict, axis=1).T.drop_duplicates().T
        df.columns = df.columns.droplevel()
        #df.index.name = "position"
        #df = df.sort_index()
        # Optional: add metadata if needed

        df.loc[:,"country_name"] = country_name
        print(df)
        debugpy.breakpoint()
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
        df.loc[valid_timestamps, 'year'] = df.loc[valid_timestamps, 'timestamp'].dt.year
        df.loc[valid_timestamps, 'quarter'] = df.loc[valid_timestamps, 'timestamp'].dt.quarter
        df.loc[valid_timestamps, 'month'] = df.loc[valid_timestamps, 'timestamp'].dt.month
        df.loc[valid_timestamps, 'day'] = df.loc[valid_timestamps, 'timestamp'].dt.day
        df.loc[valid_timestamps, 'dayofweek'] = df.loc[valid_timestamps, 'timestamp'].dt.dayofweek # Monday=0, Sunday=6
        df.loc[valid_timestamps, 'hour'] = df.loc[valid_timestamps, 'timestamp'].dt.hour
        
        # For rows where timestamp became NaT, fill with appropriate NA type
        for col in ['year', 'quarter', 'month', 'day', 'dayofweek', 'hour']:
            if df[col].isnull().any(): # Check if any NA, then fill for consistency
                 # Pandas uses pd.NA for integer NAs for some types
                 if 'int' in str(df[col].dtype).lower() or 'float' in str(df[col].dtype).lower() :
                    df[col] = df[col].astype('Int64') if 'int' in str(df[col].dtype).lower() else df[col].astype('Float64')
                 df[col] = df[col].where(valid_timestamps, pd.NA)

        return df
    @task
    def combine_df_and_params(df: pd.DataFrame, task_param: Dict[str, Any]):
        return {"df": df, "params": task_param}

    @task(task_id='load_to_staging_table_dynamic')
    def load_to_staging_table_dynamic(df_and_params: Dict[str, Any], **context) -> str:
        df = df_and_params['df']
        task_param = df_and_params['params']

        if df.empty:
            logger.info(f"Skipping load to staging for {task_param['country_name']} {task_param['periodStart']} as DataFrame is empty.")
            return f"empty_staging_{task_param['country_code']}_{task_param['periodStart']}"

        dag_run_id_safe = context['dag_run'].run_id.replace(':', '_').replace('+', '_').replace('.', '_').replace('-', '_')
        staging_table = f"stg_entsoe_{task_param['country_code']}_{task_param['periodStart']}_{dag_run_id_safe}"[:63] # Max PG table name length
        staging_table = "".join(c if c.isalnum() else "_" for c in staging_table) # Sanitize


        logger.info(f"Loading {len(df)} records to staging table: airflow_data.\"{staging_table}\" for {task_param['country_name']}")
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
        return staging_table
    
    @task(task_id='create_initial_tables_if_not_exist')
    def create_initial_tables(db_conn_id: str, prod_table: str, raw_xml_table: str) -> dict:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # Production Table
        create_prod_sql = f"""
        CREATE TABLE IF NOT EXISTS airflow_data."{prod_table}" (
            id SERIAL PRIMARY KEY, "TimeSeries_ID" TEXT, "Currency" TEXT, "Unit" TEXT, "Resolution" TEXT,
            "Position" INTEGER, "Price" NUMERIC, "timestamp" TIMESTAMP WITH TIME ZONE,
            country_name TEXT, year INTEGER, quarter INTEGER, month INTEGER, day INTEGER,
            dayofweek INTEGER, hour INTEGER, processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (timestamp, country_name));"""
        pg_hook.run(create_prod_sql)
        logger.info(f"Ensured production table airflow_data.\"{prod_table}\" exists.")
        
        # Raw XML Table
        create_raw_sql = f"""
        CREATE TABLE IF NOT EXISTS airflow_data."{raw_xml_table}" (
            id SERIAL PRIMARY KEY, xml_data XML, request_time TIMESTAMP WITH TIME ZONE, 
            request_parameters JSONB, status_code INTEGER, content_type TEXT);"""
        # Changed request_time to TIMESTAMP WITH TIME ZONE for consistency. xml_data allows NULL if extraction fails.
        pg_hook.run(create_raw_sql)
        logger.info(f"Ensured raw XML table airflow_data.\"{raw_xml_table}\" exists.")
        return {"production_table": prod_table, "raw_xml_table": raw_xml_table}


    @task(task_id='merge_to_production_table')
    def merge_data_to_production(staging_table_name: str, production_table_name: str, db_conn_id: str):
        if staging_table_name.startswith("empty_staging_"):
            logger.info(f"Skipping merge for empty/failed staging data: {staging_table_name}")
            return f"Skipped merge for {staging_table_name}"
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        merge_sql = f"""
        INSERT INTO airflow_data."{production_table_name}" (
            "TimeSeries_ID", "Currency", "Unit", "Resolution", "Position", "Price",
            timestamp, country_name, year, quarter, month, day, dayofweek, hour)
        SELECT "TimeSeries_ID", "Currency", "Unit", "Resolution", "Position", "Price",
            timestamp, country_name, year, quarter, month, day, dayofweek, hour
        FROM airflow_data."{staging_table_name}"
        WHERE timestamp IS NOT NULL AND country_name IS NOT NULL -- Ensure conflict keys are not null
        ON CONFLICT (timestamp, country_name) DO UPDATE SET
            "TimeSeries_ID" = EXCLUDED."TimeSeries_ID", "Currency" = EXCLUDED."Currency",
            "Unit" = EXCLUDED."Unit", "Resolution" = EXCLUDED."Resolution",
            "Position" = EXCLUDED."Position", "Price" = EXCLUDED."Price",
            "year" = EXCLUDED."year", "quarter" = EXCLUDED."quarter",
            "month" = EXCLUDED."month", "day" = EXCLUDED."day",
            "dayofweek" = EXCLUDED."dayofweek", "hour" = EXCLUDED."hour",
            processed_at = CURRENT_TIMESTAMP;"""
        try:
            pg_hook.run(merge_sql)
            logger.info(f"Successfully merged data from airflow_data.\"{staging_table_name}\" to airflow_data.\"{production_table_name}\".")
        except Exception as e:
            logger.error(f"Error merging data from {staging_table_name} to {production_table_name}: {e}")
            raise
        return f"Merged {staging_table_name}"

        return 0

    
    @task
    def cleanup_staging_tables(staging_table_names: List[str], db_conn_id: str):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        for table_name in staging_table_names:
            if table_name and not table_name.startswith("empty_staging_"):
                try:
                    pg_hook.run(f'DROP TABLE IF EXISTS airflow_data."{table_name}";')
                    logger.info(f"Dropped staging table: airflow_data.\"{table_name}\".")
                except Exception as e:
                    logger.error(f"Error dropping staging table {table_name}: {e}") # Log but don't fail DAG


    print('TODO - move to taskGroup one day and share some tasks for other variables, like generating units operation points')
    # A more advanced pattern could be a TaskGroup that is mapped.
    # --- Task Orchestration ---

    initial_setup = create_initial_tables(
        db_conn_id=POSTGRES_CONN_ID,
        prod_table=PRODUCTION_TABLE_NAME,
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

    staged_table_names = load_to_staging_table_dynamic.partial(
        db_conn_id=POSTGRES_CONN_ID
    ).expand(df_and_params=combined_for_staging)

    merged_results = merge_data_to_production.partial(
        production_table_name=initial_setup["production_table"], # Use XComArg from setup task
        db_conn_id=POSTGRES_CONN_ID
    ).expand(staging_table_name=staged_table_names)

    cleanup_task = cleanup_staging_tables(
        staging_table_names=staged_table_names,
        db_conn_id=POSTGRES_CONN_ID
    )
    
    #Set cleanup to run after all merges
    cleanup_task.set_upstream(merged_results)
    
    # Explicitly make sure XML storage is upstream of parsing (though usually inferred)
    parsed_dfs.set_upstream(stored_xml_ids)


entsoe_dynamic_etl_dag = entsoe_dynamic_etl_pipeline()





