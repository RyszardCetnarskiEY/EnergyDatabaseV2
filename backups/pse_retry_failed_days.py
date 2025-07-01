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

ENTITY = "pomb-rbn"
ENTITY_CONFIG = {
    "columns": [
        "ofc", "dtime", "period", "pofmax", "pofmin", "plofmax", "plofmin",
        "dtime_utc", "period_utc", "reserve_type", "business_date",
        "publication_ts", "publication_ts_utc"
    ],
    "staging_table": "stg_pomb_rbn",
    "production_table": "pomb_rbn_data"
}


@task
def get_failed_dates() -> list:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"""
    SELECT DISTINCT business_date
    FROM airflow_data.pse_api_errors
    WHERE entity = %s
    ORDER BY business_date;
    """
    result = hook.get_pandas_df(sql, parameters=(ENTITY,))
    return result["business_date"].dt.strftime("%Y-%m-%d").tolist()


@task
def fetch_retry_data(business_date: str) -> dict:
    endpoint = f"/api/{ENTITY}?$filter=business_date eq '{business_date}'"
    hook = HttpHook(method="GET", http_conn_id=HTTP_CONN_ID)
    session = hook.get_conn()
    url = hook.base_url.rstrip("/") + endpoint

    try:
        response = session.get(url)
        response.raise_for_status()
        data = response.json().get("value", [])
        if not data:
            raise ValueError(f"No data returned for {ENTITY} on {business_date}")

        df = pd.DataFrame(data)
        return {
            "business_date": business_date,
            "columns": ENTITY_CONFIG["columns"],
            "df": df.to_dict(orient="records")
        }

    except Exception as e:
        logger.error(f"[RETRY] Error for {ENTITY} on {business_date}: {e}")
        return {
            "business_date": business_date,
            "columns": ENTITY_CONFIG["columns"],
            "df": []
        }


@task
def load_to_staging(data: dict):
    df = pd.DataFrame(data["df"])
    if df.empty:
        return {"skipped": True, "business_date": data["business_date"]}

    table = ENTITY_CONFIG["staging_table"]
    cols = ENTITY_CONFIG["columns"]
    df = df[cols]

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_hook.run(f'DELETE FROM airflow_data."{table}" WHERE business_date = %s;', parameters=(data["business_date"],))

    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.copy_expert(
        f"""COPY airflow_data."{table}" ({', '.join([f'"{col}"' for col in cols])}) FROM STDIN WITH CSV HEADER""",
        file=buffer
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"skipped": False, "business_date": data["business_date"]}


@task
def merge_to_production(stage_info: dict):
    if stage_info["skipped"]:
        return f"Skipped merge for {stage_info['business_date']}"

    cols = ENTITY_CONFIG["columns"]
    staging = ENTITY_CONFIG["staging_table"]
    prod = ENTITY_CONFIG["production_table"]

    col_str = ", ".join([f'"{col}"' for col in cols])
    set_clause = ",\n".join([f'"{col}" = EXCLUDED."{col}"' for col in cols])

    sql = f"""
    INSERT INTO airflow_data."{prod}" ({col_str})
    SELECT {col_str} FROM airflow_data."{staging}"
    WHERE business_date = %s
    ON CONFLICT (business_date, ofc, period) DO UPDATE SET
    {set_clause},
    processed_at = CURRENT_TIMESTAMP;
    """

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_hook.run(sql, parameters=(stage_info["business_date"],))
    return f"Merged {stage_info['business_date']}"


@task
def cleanup_and_remove_error(stage_info: dict):
    if stage_info["skipped"]:
        return

    table = ENTITY_CONFIG["staging_table"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Truncate
    hook.run(f'DELETE FROM airflow_data."{table}" WHERE business_date = %s;', parameters=(stage_info["business_date"],))

    # Delete error log
    hook.run(
        f"DELETE FROM airflow_data.pse_api_errors WHERE entity = %s AND business_date = %s;",
        parameters=(ENTITY, stage_info["business_date"])
    )


@dag(
    dag_id="pse_retry_failed_days",
    schedule=None,  # manual trigger only
    start_date=datetime(2024, 6, 14),
    catchup=False,
    tags=["pse", "retry", "errors"]
)
def pse_retry_failed_days():
    failed_dates = get_failed_dates()
    retry_data = fetch_retry_data.expand(business_date=failed_dates)
    staging_info = load_to_staging.expand(data=retry_data)
    merge_results = merge_to_production.expand(stage_info=staging_info)
    cleanup = cleanup_and_remove_error.expand(stage_info=staging_info)
    cleanup.set_upstream(merge_results)

#dag = pse_retry_failed_days()
