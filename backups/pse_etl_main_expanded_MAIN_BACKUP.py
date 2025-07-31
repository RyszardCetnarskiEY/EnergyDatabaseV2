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
HISTORICAL_START_DATE = datetime(2025, 6, 1)

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
def create_entity_tables(task_param: dict):
    entity = task_param["entity"]
    columns = task_param["columns"]
    cols_sql = ",\n".join([f'"{col}" TEXT' for col in columns])
    unique_cols = None if entity in ["poeb-rbn", "poeb-rbb", "pomb-rbn"] else '"business_date", "dtime"'

    staging_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{task_param['staging_table']}" (
        id SERIAL PRIMARY KEY,
        {cols_sql},
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    production_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{task_param['production_table']}" (
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
def create_log_table():
    sql = """
    CREATE TABLE IF NOT EXISTS airflow_data.pse_api_log (
        id SERIAL PRIMARY KEY,
        entity TEXT,
        business_date DATE,
        message TEXT,
        result TEXT CHECK (result IN ('success', 'fail')),
        logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (entity, business_date)
    );
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(sql)

@task
def filter_entities_to_run(task_params: list, execution_date=None) -> list:
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    date = execution_date.strftime("%Y-%m-%d")
    return [
        param for param in task_params
        if not pg.get_first(
            """
            SELECT 1 FROM airflow_data.pse_api_log
            WHERE entity = %s AND business_date = %s AND result = 'success'
            """,
            parameters=(param['entity'], date)
        )
    ]

@task
def fetch_entity_data(task_param: dict, execution_date=None) -> dict:
    entity = task_param["entity"]
    columns = task_param["columns"]
    date_str = execution_date.strftime("%Y-%m-%d")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    log_check_sql = """
        SELECT result, message FROM airflow_data.pse_api_log
        WHERE entity = %s AND business_date = %s
        ORDER BY logged_at DESC
        LIMIT 1;
    """
    log_result = pg_hook.get_first(log_check_sql, parameters=(entity, date_str))

    if log_result:
        result_status, previous_message = log_result
        if result_status == "success":
            # Pomijamy, bo już wcześniej się udało
            return {
                "entity": entity,
                "columns": columns,
                "df": [],
                "skipped": True,
                "skip_reason": "Already fetched successfully previously."
            }

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
            "columns": columns,
            "df": df.to_dict(orient="records")
        }
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error fetching data for {entity} on {date_str}: {error_message}")

         # Jeśli już był fail, aktualizujemy message
        if log_result and result_status == "fail":
            update_sql = """
                UPDATE airflow_data.pse_api_log
                SET message = %s, logged_at = CURRENT_TIMESTAMP
                WHERE entity = %s AND business_date = %s;
            """
            pg_hook.run(update_sql, parameters=(error_message, entity, date_str))

        return {
            "entity": entity,
            "columns": columns,
            "df": [],
            "error": error_message
        }

@task
def load_to_staging(data: dict):
    if data.get("skipped"):
        return {
            "staging_table": None,
            "entity": data["entity"],
            "skipped": True,
            "error": data.get("skip_reason")
        }

    entity = data["entity"]
    columns = data["columns"]
    df = pd.DataFrame(data["df"])

    if df.empty:
        return {"staging_table": None, "entity": entity, "skipped": True, "error": data.get("error")}

    staging_table = [v["staging_table"] for k, v in ENTITY_CONFIG.items() if k == entity][0]
    df = df[columns]

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
        return

    entity = staging_info["entity"]
    config = ENTITY_CONFIG[entity]
    prod_table = config["production_table"]
    staging_table = config["staging_table"]
    columns = config["columns"]

    col_str = ", ".join([f'"{col}"' for col in columns])
    set_clause = ",\n".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    if entity == "poeb-rbb":
        sql = f"""
        INSERT INTO airflow_data."{prod_table}" ({col_str})
        SELECT {col_str}
        FROM airflow_data."{staging_table}";
        """
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
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql)

@task
def cleanup_staging(staging_info: dict):
    if staging_info["skipped"]:
        return
    table = staging_info["staging_table"]
    PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(f'TRUNCATE airflow_data."{table}";')

@task
def log_etl_result(staging_info: dict, execution_date=None):
    entity = staging_info["entity"]
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    date_str = execution_date.strftime("%Y-%m-%d")

    if staging_info["skipped"]:
        message = staging_info.get("error", "No data fetched or staged")
        
        if message.startswith("Already fetched successfully"):
            result = "success"
        else:
            result = "fail"

    else:
        try:
            prod_table = ENTITY_CONFIG[entity]["production_table"]
            sql = f'SELECT COUNT(*) FROM airflow_data."{prod_table}" WHERE business_date = %s;'
            count = pg_hook.get_first(sql, parameters=(date_str,))[0]
            result = "success" if count > 0 else "fail"
            message = "Loaded" if count > 0 else "No data after merge"
        except Exception as e:
            result = "fail"
            message = f"Check error: {str(e)}"

    log_sql_upsert = """
        INSERT INTO airflow_data.pse_api_log (entity, business_date, message, result)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (entity, business_date)
        DO UPDATE SET
            message = EXCLUDED.message,
            result = EXCLUDED.result,
            logged_at = CURRENT_TIMESTAMP;
    """
    pg_hook.run(log_sql_upsert, parameters=(entity, date_str, message, result))


@dag(
    dag_id="pse_etl_main_expand",
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
def pse_etl_main_expand():
    log_table = create_log_table()

    # Parametry jako lista dictów
    raw_task_params = [{"entity": k, **v} for k, v in ENTITY_CONFIG.items()]
    task_params = filter_entities_to_run(raw_task_params)
    task_params.set_upstream(log_table)

    tables = create_entity_tables.expand(task_param=task_params)
    fetched_data = fetch_entity_data.expand(task_param=task_params)
    fetched_data.set_upstream(tables)

    staged = load_to_staging.expand(data=fetched_data)
    merged = merge_to_production.expand(staging_info=staged)
    cleanup = cleanup_staging.expand(staging_info=staged)
    cleanup.set_upstream(merged)
    log_result = log_etl_result.expand(staging_info=staged)
    log_result.set_upstream(merged)

    for task in [tables, fetched_data, staged, merged, cleanup, log_result]:
        task.set_upstream(log_table)

#dag = pse_etl_main_expand()
