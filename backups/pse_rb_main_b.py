from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import StringIO
import pandas as pd
import logging


logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_azure_vm"
HTTP_CONN_ID = "PSE_API"
HISTORICAL_START_DATE = datetime(2025, 6, 1) # API PSE - Data początkowa

ENTITY_CONFIG_MAIN = {
    "pomb-rbn": {
        "columns": [
            "ofc", "dtime", "period", "pofmax", "pofmin", "plofmax", "plofmin",
            "dtime_utc", "period_utc", "reserve_type", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_pomb_rbn",
        "production_table": "pomb_rbn"
    },
    "kmb-kro-rozl": {
        "columns": [
            "kmb", "kro", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_kmb_kro_rozl",
        "production_table": "kmb_kro_rozl"
    },
    "poeb-rbn": {
        "columns": [
            "ofp", "ofcd", "ofcg", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_poeb_rbn",
        "production_table": "poeb_rbn"
    },
    "poeb-rbb": {
        "columns": [
            "ofp", "ofcd", "ofcg", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_poeb_rbb",
        "production_table": "poeb_rbb"
    },
    "crb-rozl": {
        "columns": [
            "dtime", "period", "cen_cost", "dtime_utc", "ckoeb_cost",
            "period_utc", "ceb_pp_cost", "ceb_sr_cost",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_crb_rozl",
        "production_table": "crb_rozl"
    },
    "zmb": {
        "columns": [
            "dtime", "period", "zmb_rrd", "zmb_rrg", "zmb_fcrd", "zmb_fcrg", "zmb_frrd",
            "zmb_frrg", "dtime_utc", "zmb_afrrd", "zmb_afrrg", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_zmb",
        "production_table": "zmb"
    },
    "cmbp-tp": {
        "columns": [
            "onmb", "rr_d", "rr_g", "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g",
            "mfrrd_d", "mfrrd_g", "dtime_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_cmbp_tp",
        "production_table": "cmbp_tp"
    },
    "mbu-tu": {
        "columns": [
            "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g", "period", "mfrrd_d", "mfrrd_g",
            "dtime_utc", "period_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_mbu_tu",
        "production_table": "mbu_tu"
    },
    "mbp-tp": {
        "columns": [
            "rr_d", "rr_g", "dtime", "fcr_d", "fcr_g", "onmbp", "afrr_d", "afrr_g", "mfrrd_d",
            "mfrrd_g", "dtime_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_mbp_tp",
        "production_table": "mbp_tp"
    },
    "rce-pln": {
        "columns": [
            "dtime", "period", "rce_pln", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_rce_pln",
        "production_table": "rce_pln"
    },
    "csdac-pln": {
        "columns": [
            "dtime", "period", "csdac_pln", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_csdac_pln",
        "production_table": "csdac_pln"
    },
    "eb-rozl": {
        "columns": [
            "dtime", "period", "eb_d_pp", "eb_w_pp", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_eb_rozl",
        "production_table": "eb_rozl"
    },
    "cmbu-tu": {
        "columns": [
            "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g", "period", "mfrrd_d", "mfrrd_g",
            "dtime_utc", "period_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_cmbu_tu",
        "production_table": "cmbu_tu"
    },
    "popmb-rmb": {
        "columns": [
            "com", "pom", "comrr", "dtime", "onmbp", "dtime_utc", "reserve_type",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_popmb_rmb",
        "production_table": "popmb_rmb"
    }
}

ENTITY_CONFIG = {
    "poeb-rbb": {
        "columns": [
            "ofp", "ofcd", "ofcg", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_poeb_rbb",
        "production_table": "poeb_rbb"
    },
    "zmb": {
        "columns": [
            "dtime", "period", "zmb_rrd", "zmb_rrg", "zmb_fcrd", "zmb_fcrg", "zmb_frrd",
            "zmb_frrg", "dtime_utc", "zmb_afrrd", "zmb_afrrg", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_zmb",
        "production_table": "zmb"
    }
}


@task
def create_entity_tables(entity: str):
    config = ENTITY_CONFIG[entity]
    cols_sql = ",\n".join([f'"{col}" TEXT' for col in config["columns"]])

    if entity in ["poeb-rbn", "poeb-rbb", "pomb-rbn"]:
        unique_cols = None  # NIE dodajemy UNIQUE dla ofert
    else:
        unique_cols = '"business_date", "dtime"'


    staging_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{config['staging_table']}" (
        id SERIAL PRIMARY KEY,
        {cols_sql},
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    production_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{config['production_table']}" (
        id SERIAL PRIMARY KEY,
        {cols_sql},
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        {" ,UNIQUE (" + unique_cols + ")" if unique_cols else ""}
    );
    """


    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(staging_sql)
    hook.run(production_sql)

@task
def create_error_log_table():
    sql = """
    CREATE TABLE IF NOT EXISTS airflow_data.pse_api_errors (
        id SERIAL PRIMARY KEY,
        entity TEXT,
        business_date DATE,
        error_message TEXT,
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(sql)

@task
def fetch_entity_data(entity: str, execution_date=None) -> dict:
    date_str = execution_date.strftime("%Y-%m-%d")
    endpoint = f"/api/{entity}?$filter=business_date eq '{date_str}'"

    hook = HttpHook(method="GET", http_conn_id=HTTP_CONN_ID)
    session = hook.get_conn()
    url = hook.base_url.rstrip("/") + endpoint

    try:
        response = session.get(url)
        response.raise_for_status()
        data = response.json().get("value", [])

        if not data:
            raise ValueError(f"No data returned for {entity} on {date_str}")

        df = pd.DataFrame(data)
        return {
            "entity": entity,
            "columns": ENTITY_CONFIG[entity]["columns"],
            "df": df.to_dict(orient="records")
        }

    except Exception as e:
        logger.error(f"Error fetching data for {entity} on {date_str}: {str(e)}")

        # Zapisz do pse_api_errors
        sql = """
        INSERT INTO airflow_data.pse_api_errors (entity, business_date, error_message)
        VALUES (%s, %s, %s);
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql, parameters=(entity, date_str, str(e)))

        # Zwróć pusty df, żeby nie przerywać DAG-a
        return {
            "entity": entity,
            "columns": ENTITY_CONFIG[entity]["columns"],
            "df": []
        }

@task
def load_to_staging(data: dict):
    entity = data["entity"]
    columns = data["columns"]
    staging_table = ENTITY_CONFIG[entity]["staging_table"]
    df = pd.DataFrame(data["df"])

    if df.empty:
        logger.info(f"No data to stage for {entity}")
        return {"staging_table": staging_table, "entity": entity, "skipped": True}

    df = df[columns]

    # Bufor CSV
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    business_date = df["business_date"].iloc[0]
    pg_hook.run(f'DELETE FROM airflow_data."{staging_table}" WHERE business_date = %s;', parameters=(business_date,))

    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.copy_expert(
        f"""COPY airflow_data."{staging_table}" ({', '.join([f'"{c}"' for c in columns])}) FROM STDIN WITH CSV HEADER""",
        file=buffer
    )

    conn.commit()
    cur.close()
    conn.close()

    return {"staging_table": staging_table, "entity": entity, "skipped": False}

@task
def merge_to_production(staging_info: dict):
    if staging_info["skipped"]:
        logger.info(f"Skipping merge for {staging_info['entity']}")
        return

    entity = staging_info["entity"]
    config = ENTITY_CONFIG[entity]
    prod_table = config["production_table"]
    staging_table = config["staging_table"]
    columns = config["columns"]

    set_clause = ",\n".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])
    col_str = ", ".join([f'"{col}"' for col in columns])

    if entity == "poeb-rbn" or entity == "poeb-rbb":
        conflict_cols = '"business_date", "dtime", "ofp", "ofcd", "ofcg"'
    elif entity == "pomb-rbn":
        conflict_cols = '"business_date", "dtime", "ofc", "reserve_type"'
    else:
        conflict_cols = '"business_date", "dtime"'

    if entity in ["poeb-rbn", "poeb-rbb", "pomb-rbn"]:
        # oferty: Nie mają ON CONFLICT, bo mogą się powtarzać — brak deduplikacji
        sql = f"""
        INSERT INTO airflow_data."{prod_table}" ({col_str})
        SELECT {col_str}
        FROM airflow_data."{staging_table}";
        """
    else:
        sql = f"""
        INSERT INTO airflow_data."{prod_table}" ({col_str})
        SELECT {col_str}
        FROM airflow_data."{staging_table}"
        ON CONFLICT ({conflict_cols}) DO UPDATE SET
        {set_clause},
        processed_at = CURRENT_TIMESTAMP;
        """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(sql)

@task
def cleanup_staging(staging_info: dict):
    if staging_info["skipped"]:
        return
    table = ENTITY_CONFIG[staging_info["entity"]]["staging_table"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(f'TRUNCATE airflow_data."{table}";')

@dag(
    dag_id="pse_etl_main",
    schedule_interval="@daily",
    start_date=HISTORICAL_START_DATE,
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["pse", "staging", "merge"]
)
def pse_etl_main():
    error_table = create_error_log_table()

    for entity in ENTITY_CONFIG:
        tables = create_entity_tables(entity)
        data = fetch_entity_data(entity)
        data.set_upstream([tables, error_table])

        staged = load_to_staging(data)
        merged = merge_to_production(staged)
        cleanup = cleanup_staging(staged)
        cleanup.set_upstream(merged)

#dag = pse_etl_main()
