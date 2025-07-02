from datetime import timedelta
from pendulum import datetime
import json
import logging
import pandas as pd
from typing import Dict, Any, List
import xml.etree.ElementTree as ET
from io import StringIO

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime as dt
from pandas import DataFrame


logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "raw_entsoe_generation_units"

COUNTRY_MAPPING = {
    "PL": {"name": "Poland", "BiddingZone_Domain": ["10YPL-AREA-----S"]},
    "DE": {"name": "Germany", "BiddingZone_Domain": ["10Y1001A1001A82H"]},
    "CZ": {"name": "Czech_Republic", "BiddingZone_Domain": ["10YCZ-CEPS-----N"]},
}

ENTSOE_VARIABLES = {
    "Production and Generation Units": {
        "AreaType": "BiddingZone_Domain",
        "params": {"documentType": "A95", "businessType": "B11"},
    }
}

HISTORICAL_START_DATE = datetime(2021, 1, 1, tz="UTC")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@task(task_id='create_initial_tables_if_not_exist')
def create_initial_tables(db_conn_id: str, raw_xml_table: str) -> dict:
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    create_raw_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{raw_xml_table}" (
        id SERIAL PRIMARY KEY,
        var_name TEXT,
        country_name TEXT,
        area_code TEXT,
        status_code INTEGER,
        period_start DATE,
        request_time TIMESTAMPTZ,
        xml_data XML,
        request_parameters JSONB,
        content_type TEXT
    );"""
    pg_hook.run(create_raw_sql)
    return {"raw_xml_table": raw_xml_table}

def _generate_run_parameters_logic(year):
    task_params = []
    for var_name, entsoe_params_dict in ENTSOE_VARIABLES.items():
        for country_code, country_details in COUNTRY_MAPPING.items():
            for in_domain in country_details[entsoe_params_dict["AreaType"]]:
                task_params.append({
                    "entsoe_api_params": {
                        "Implementation_DateAndOrTime": f"{year}-01-01",
                        "BiddingZone_Domain": in_domain,
                        **entsoe_params_dict["params"],
                    },
                    "task_run_metadata": {
                        "var_name": var_name,
                        "config_dict": entsoe_params_dict,
                        "country_code": country_code,
                        "country_name": country_details["name"],
                    }
                })
    return task_params

"""
@task
def generate_run_parameters(**context) -> List[Dict[str, str]]:
    year = context['data_interval_start'].year
    return _generate_run_parameters_logic(year)
"""

@task
def generate_run_parameters(logical_date: dt) -> List[Dict[str, str]]:
    year = logical_date.year
    return _generate_run_parameters_logic(year)


@task(task_id='extract_from_api')
def extract_from_api(task_param: Dict[str, Any], **context) -> Dict[str, Any]:
    entsoe_api_params = task_param["entsoe_api_params"]
    task_run_metadata = task_param["task_run_metadata"]

    http_hook = HttpHook(method='GET', http_conn_id="ENTSOE")
    conn = http_hook.get_connection("ENTSOE")
    api_key = conn.password
    base_url = conn.host.rstrip("/")

    request_params = {"securityToken": api_key, **entsoe_api_params}

    try:
        session = http_hook.get_conn()
        response = session.get(base_url, params=request_params, timeout=60)
        response.raise_for_status()

        return {
            "xml_content": response.text,
            "status_code": response.status_code,
            "country_name": task_run_metadata["country_name"],
            "area_code": entsoe_api_params["BiddingZone_Domain"],
            "var_name": task_run_metadata["var_name"],
            "period_start": entsoe_api_params["Implementation_DateAndOrTime"],
            "request_time": context['logical_date'].isoformat(),
            "request_params": json.dumps(request_params),
            "content_type": response.headers.get("Content-Type"),
            "year_api": int(entsoe_api_params["Implementation_DateAndOrTime"].split("-")[0])  # ✅ DODANE
        }

    except Exception as e:
        logger.error(f"Request failed: {e}")
        raise


@task(task_id='store_raw_xml')
def store_raw_xml(extracted_data: Dict[str, Any], db_conn_id: str, table_name: str) -> int:
    if not extracted_data or 'xml_content' not in extracted_data:
        return -1

    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    sql = f"""
    INSERT INTO airflow_data."{table_name}"
    (var_name, country_name, area_code, status_code, period_start,
     request_time, xml_data, request_parameters, content_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    RETURNING id;
    """

    values = (
        extracted_data['var_name'],
        extracted_data['country_name'],
        extracted_data['area_code'],
        extracted_data['status_code'],
        pd.to_datetime(extracted_data['period_start']).date(),
        extracted_data['request_time'],
        extracted_data['xml_content'],
        extracted_data['request_params'],
        extracted_data['content_type'],
    )

    try:
        inserted_id = pg_hook.run(sql, parameters=values, handler=lambda cursor: cursor.fetchone()[0])
        return inserted_id
    except Exception as e:
        logger.error(f"Błąd przy zapisie XML: {e}")
        raise


@task(task_id='parse_generation_units_metadata')
def parse_generation_units_metadata(extracted_data: Dict[str, Any]) -> pd.DataFrame:
    xml_data = extracted_data['xml_content']
    country = extracted_data['country_name']
    implementation_date = extracted_data['period_start']
    year_api = extracted_data['year_api']  # ✅ UŻYCIE PRZEKAZANEGO PARAMETRU

    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        raise ValueError(f"Invalid XML: {e}")

    ns = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
    units = []

    for ts in root.findall('ns:TimeSeries', ns):
        plant_name = ts.findtext('ns:registeredResource.name', namespaces=ns)
        plant_mrid = ts.findtext('ns:registeredResource.mRID', namespaces=ns)
        plant_location = ts.findtext('ns:registeredResource.location.name', namespaces=ns)
        bidding_zone = ts.findtext('ns:biddingZone_Domain.mRID', namespaces=ns)
        psr_type = ts.findtext('ns:MktPSRType/ns:psrType', namespaces=ns)
        impl_date = ts.findtext('ns:implementation_DateAndOrTime.date', namespaces=ns)

        gu_elements = ts.findall('.//ns:GeneratingUnit_PowerSystemResources', ns)

        if gu_elements:
            for gu in gu_elements:
                unit = {
                    'country': country,
                    'implementation_date': impl_date or implementation_date,
                    'plant_name': plant_name,
                    'plant_mrid': plant_mrid,
                    'plant_location': plant_location,
                    'bidding_zone': bidding_zone,
                    'psr_type': psr_type,
                    'unit_id': gu.findtext('ns:mRID', namespaces=ns),
                    'unit_name': gu.findtext('ns:name', namespaces=ns),
                    'nominalP': gu.findtext('ns:nominalP', namespaces=ns),
                    'unit_psr_type': gu.findtext('ns:generatingUnit_PSRType.psrType', namespaces=ns),
                    'unit_location': gu.findtext('ns:generatingUnit_Location.name', namespaces=ns),
                    'year_api': year_api  # ✅ POPRAWKA
                }
                try:
                    unit['nominalP'] = float(str(unit['nominalP']).replace(',', '.')) if unit['nominalP'] else None
                except:
                    unit['nominalP'] = None
                units.append(unit)
        else:
            fallback_nominalP = ts.findtext('ns:nominalIP_PowerSystemResources.nominalP', namespaces=ns)
            unit = {
                'country': country,
                'implementation_date': impl_date or implementation_date,
                'plant_name': plant_name,
                'plant_mrid': plant_mrid,
                'plant_location': plant_location,
                'bidding_zone': bidding_zone,
                'psr_type': psr_type,
                'unit_id': plant_mrid,
                'unit_name': plant_name + " (aggregated)",
                'nominalP': fallback_nominalP,
                'unit_psr_type': psr_type,
                'unit_location': plant_location,
                'year_api': year_api  # ✅ POPRAWKA
            }
            try:
                unit['nominalP'] = float(str(unit['nominalP']).replace(',', '.')) if unit['nominalP'] else None
            except:
                unit['nominalP'] = None
            units.append(unit)

    df = pd.DataFrame(units)
    expected_columns = [
        'country', 'implementation_date', 'plant_name', 'plant_mrid', 'plant_location',
        'bidding_zone', 'psr_type', 'unit_id', 'unit_name', 'nominalP',
        'unit_psr_type', 'unit_location', 'year_api'
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    return df



#✅ 8. Tworzy listę słowników: { "df": ..., "task_param": ... }
@task
def zip_df_and_params(dfs: list, metadata_list: list) -> list[dict]:
    if len(dfs) != len(metadata_list):
        raise ValueError(f"Cannot zip: len(dfs)={len(dfs)} vs len(params)={len(metadata_list)}")
    return [{"df": df_obj, "task_param": param} for df_obj, param in zip(dfs, metadata_list)]


#✅ 9.
@task(task_id='load_to_staging_table')
def load_to_staging_table(df_and_params: dict, db_conn_id: str, **context) -> dict:
    df = df_and_params['df']
    task_param = df_and_params['task_param']

    if df.empty:
        country = task_param['task_run_metadata']['country_name']
        period = task_param['entsoe_api_params']['Implementation_DateAndOrTime']
        logger.info(f"Skipping load to staging for {country} {period} as DataFrame is empty.")
        return {
            "staging_table_name": None,
            "reason": "empty",
            "country_code": task_param['task_run_metadata']['country_code'],
            "period": period
        }

    if 'nominalP' in df.columns:
        df['nominalP'] = df['nominalP'].apply(
            lambda x: float(str(x).replace(",", ".")) if pd.notnull(x) else None
        )

    # Generowanie bezpiecznej nazwy tabeli stagingowej
    country_code = task_param['task_run_metadata']['country_code']
    #period_start = task_param['entsoe_api_params']['periodStart']
    period_start = task_param['entsoe_api_params']['Implementation_DateAndOrTime']
    dag_run_id_safe = context['dag_run'].run_id.replace(':', '_').replace('+', '_').replace('.', '_').replace('-', '_')
    staging_table = f"stg_entsoe_{country_code}_{period_start}_{dag_run_id_safe}"[:63]
    staging_table = "".join(c if c.isalnum() else "_" for c in staging_table)

    logger.info(f"Loading {len(df)} records to staging table: airflow_data.\"{staging_table}\"")

    # 🔌 Połączenie do PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)

    # 🧱 Tworzenie struktury tabeli na podstawie typów kolumn
    create_table_columns = []
    for col_name, dtype in df.dtypes.items():
        if "int" in str(dtype): pg_type = "INTEGER"
        elif "float" in str(dtype): pg_type = "NUMERIC"
        elif "datetime" in str(dtype): pg_type = "TIMESTAMP WITH TIME ZONE"
        else: pg_type = "TEXT"
        create_table_columns.append(f'"{col_name}" {pg_type}')

    # 🗃️ DROP + CREATE tabeli stagingowej
    drop_stmt = f'DROP TABLE IF EXISTS airflow_data."{staging_table}";'
    create_stmt = f'CREATE TABLE airflow_data."{staging_table}" (id SERIAL PRIMARY KEY, {", ".join(create_table_columns)}, processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);'

    pg_hook.run(drop_stmt)
    pg_hook.run(create_stmt)

    # 📤 Eksport danych do CSV w pamięci
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # 🚀 COPY z CSV do PostgreSQL
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    sql_columns = ', '.join([f'"{col}"' for col in df.columns])

    try:
        cur.copy_expert(
            sql=f"""COPY airflow_data."{staging_table}" ({sql_columns}) FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '"'""",
            file=csv_buffer
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    return {
        "staging_table_name": staging_table,
        "var_name": task_param['task_run_metadata']['var_name'],
        "country_code": country_code,
        "period": period_start
    }


#✅ 10.
@task(task_id='merge_to_production_table')
def merge_data_to_production(staging_dict: Dict[str, Any], db_conn_id: str) -> str:
    staging_table_name = staging_dict['staging_table_name']
    if not staging_table_name or staging_table_name.startswith("empty_staging_"):
        logger.info(f"Skipping merge for empty or failed staging table: {staging_table_name}")
        return f"Skipped: {staging_table_name}"

    production_table_name = "generation_units_MAIN"
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)

    # 🔧 Utwórz tabelę produkcyjną, jeśli nie istnieje
    #unit_id TEXT UNIQUE,
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{production_table_name}" (
        id SERIAL PRIMARY KEY,
        country TEXT,
        implementation_date TEXT,
        plant_name TEXT,
        plant_mrid TEXT,
        plant_location TEXT,
        bidding_zone TEXT,
        psr_type TEXT,
        unit_id TEXT,
        unit_name TEXT,
        "nominalP" NUMERIC,
        unit_psr_type TEXT,
        unit_location TEXT,
        year_api INTEGER,
        processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook.run(create_sql)

    # 🔁 MERGE (UPSERT) – jeśli unit_id już istnieje, aktualizuj rekord
    merge_sql = f"""
    INSERT INTO airflow_data."{production_table_name}" (
    country, implementation_date, plant_name, plant_mrid, plant_location,
    bidding_zone, psr_type, unit_id, unit_name, "nominalP",
    unit_psr_type, unit_location, year_api
    )
    SELECT 
        country, implementation_date, plant_name, plant_mrid, plant_location,
        bidding_zone, psr_type, unit_id, unit_name, "nominalP",
        unit_psr_type, unit_location, year_api
    FROM airflow_data."{staging_table_name}";
    """

    try:
        pg_hook.run(merge_sql)
        logger.info(f"Merged data from staging '{staging_table_name}' to production '{production_table_name}'")
    except Exception as e:
        logger.error(f"Merge failed from {staging_table_name} → {production_table_name}: {e}")
        raise

    return f"Merged to {production_table_name}"


# ⚖️ 10. Cleanup task for staging tables
@task
def cleanup_staging_tables(staging_dict: Dict[str, Any], db_conn_id: str):
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    table_name = staging_dict['staging_table_name']
    if table_name and not table_name.startswith("empty_staging_"):
        try:
            pg_hook.run(f'DROP TABLE IF EXISTS airflow_data."{table_name}";')
            logger.info(f"Dropped staging table: airflow_data.\"{table_name}\".")
        except Exception as e:
            logger.error(f"Error dropping staging table {table_name}: {e}")



@dag(
    dag_id='entsoe_generators_db',
    default_args=default_args,
    schedule='@yearly',
    start_date=HISTORICAL_START_DATE,
    catchup=True,
    tags=['entsoe', 'energy', 'api', 'etl', 'dynamic', 'generators'],
    max_active_runs=1,
    doc_md="""
        ### ENTSO-E Generation Metadata Pipeline. Pobiera dane z ENTSO-E (documentType=A95) i ładuje je do PostgreSQL.
        Zawiera dynamiczne taski z .expand i staging table logic.
    """
)
def entsoe_gen_db_pipeline():

    initial_setup = create_initial_tables(
        db_conn_id=POSTGRES_CONN_ID,
        raw_xml_table=RAW_XML_TABLE_NAME
    )

    task_parameters = generate_run_parameters()
    task_parameters.set_upstream(initial_setup)

    extracted = extract_from_api.expand(task_param=task_parameters)

    stored_xml_ids = store_raw_xml.partial(
        db_conn_id=POSTGRES_CONN_ID,
        table_name=RAW_XML_TABLE_NAME
    ).expand(extracted_data=extracted)

    parsed_units = parse_generation_units_metadata.expand(extracted_data=extracted)

    #merged = merge_all_dataframes(parsed_units)

    combined_for_staging = zip_df_and_params(dfs=parsed_units, metadata_list=task_parameters)

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
    parsed_units.set_upstream(stored_xml_ids)



entsoe_gen_db_dag = entsoe_gen_db_pipeline()