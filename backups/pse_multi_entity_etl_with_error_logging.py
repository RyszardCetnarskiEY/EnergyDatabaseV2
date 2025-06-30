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
HISTORICAL_START_DATE = datetime(2024, 6, 14) # Data poczatkowa api pse
#HISTORICAL_START_DATE = datetime(2025, 6, 20) # Data konkretna



ENTITY_CONFIG = {
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
    }
}


@task
def create_entity_tables(entity: str):
    config = ENTITY_CONFIG[entity]
    cols_sql = ",\n".join([f'"{col}" TEXT' for col in config["columns"]])

    # Wyjątki: encje ofertowe – muszą mieć bardziej złożony UNIQUE
    if entity == "poeb-rbn" or entity == "poeb-rbb":
        unique_cols = '"business_date", "dtime", "ofp", "ofcd", "ofcg"'
    elif entity == "pomb-rbn":
        unique_cols = '"business_date", "dtime", "ofc", "reserve_type"'
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
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE ({unique_cols})
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

    # Deduplikacja danych w stagingu
    if entity == "poeb-rbn" or entity == "poeb-rbb":
        before = len(df)
        df = df.drop_duplicates(subset=["business_date", "dtime", "ofp", "ofcd", "ofcg"])
        after = len(df)
        logger.info(f"Deduplicated {before - after} rows for {entity}")
    elif entity == "pomb-rbn":
        before = len(df)
        df = df.drop_duplicates(subset=["business_date", "dtime", "ofc", "reserve_type"])
        after = len(df)
        logger.info(f"Deduplicated {before - after} rows for {entity}")
    else:
        before = len(df)
        df = df.drop_duplicates(subset=["business_date", "dtime"])
        after = len(df)
        logger.info(f"Deduplicated {before - after} rows for {entity}")



    # Przygotowanie bufora CSV
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    # Połącz się z bazą danych
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Usuń stare dane z tego samego dnia (dla poprawności)
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

    # Ustalamy konfliktowe kolumny wg typu encji
    if entity == "poeb-rbn" or entity == "poeb-rbb":
        conflict_cols = '"business_date", "dtime", "ofp", "ofcd", "ofcg"'
    elif entity == "pomb-rbn":
        conflict_cols = '"business_date", "dtime", "ofc", "reserve_type"'
    else:
        conflict_cols = '"business_date", "dtime"'

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
    dag_id="pse_etl_staging_merge",
    schedule_interval="@daily",
    start_date=HISTORICAL_START_DATE,
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["pse", "staging", "merge"]
)
def pse_etl_staging_merge():
    error_table = create_error_log_table()

    for entity in ENTITY_CONFIG:
        tables = create_entity_tables(entity)
        data = fetch_entity_data(entity)
        data.set_upstream([tables, error_table])

        staged = load_to_staging(data)
        merged = merge_to_production(staged)
        cleanup = cleanup_staging(staged)
        cleanup.set_upstream(merged)

#dag = pse_etl_staging_merge()
