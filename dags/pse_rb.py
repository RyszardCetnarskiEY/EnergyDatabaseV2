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
HISTORICAL_START_DATE = datetime(2024, 6, 14) # Od tego dnia są dane z API PSE !
#HISTORICAL_START_DATE = datetime(2025, 5, 24)

ENTITY_CONFIG = {
    # zmb - Zapotrzebowanie na moce bilansujące nabywane w ramach RMB
    "zmb": {
        "columns": [
            "dtime", "period", "zmb_rrd", "zmb_rrg", "zmb_fcrd", "zmb_fcrg", "zmb_frrd",
            "zmb_frrg", "dtime_utc", "zmb_afrrd", "zmb_afrrg", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "zmb_data"
    },
    # cmbp-tp - Ceny mocy bilansujących nabytych w trybie podstawowym
    "cmbp-tp": {
        "columns": [
            "onmb", "rr_d", "rr_g", "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g",
            "mfrrd_d", "mfrrd_g", "dtime_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "cmbp_tp_data"
    },
    # mbu-tu - Wielkość mocy bilansujących nabytych w trybie uzupełniającym
    "mbu-tu": {
        "columns": [
            "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g", "period", "mfrrd_d", "mfrrd_g",
            "dtime_utc", "period_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "mbu_tu_data"
    },
    # mbp-tp - Wielkość mocy bilansujących nabytych w trybie podstawowym
    "mbp-tp": {
        "columns": [
            "rr_d", "rr_g", "dtime", "fcr_d", "fcr_g", "onmbp", "afrr_d", "afrr_g", "mfrrd_d",
            "mfrrd_g", "dtime_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "mbp_tp_data"
    },
    "rce-pln": {
        "columns": [
            "dtime", "period", "rce_pln", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "rce_pln_data"
    },
    "csdac-pln": {
        "columns": [
            "dtime", "period", "csdac_pln", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "csdac_pln_data"
    },
    # eb-rozl - Ilość energii bilansującej aktywowanej (do porównania z sumą pkt 3 i 6)
    "eb-rozl": {
        "columns": [
            "dtime", "period", "eb_d_pp", "eb_w_pp", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "eb_rozl_data"
    },
    # cmbu-tu - Ceny mocy bilansującej nabytych w trybie uzupełniającym
    "cmbu-tu": {
        "columns": [
            "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g", "period", "mfrrd_d", "mfrrd_g",
            "dtime_utc", "period_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "cmbu_tu_data"
    },
    # popmb-rmb - Oferty portfolio na Moce Bilansujące
    "popmb-rmb": {
        "columns": [
            "com", "pom", "comrr", "dtime", "onmbp", "dtime_utc", "reserve_type",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "popmb_rmb_data"
    },
    # pomb-rbn - Oferty na moce bilansujące przyjęte na RBN
    "pomb-rbn": {
        "columns": [
            "ofc", "dtime", "period", "pofmax", "pofmin", "plofmax", "plofmin",
            "dtime_utc", "period_utc", "reserve_type", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "table": "pomb_rbn_data"
    },
    # crb-rozl - Ceny na rb, w tym: CEN, CKOEB, CEBśr, CEPpp
    "crb-rozl": {
        "columns": [
            "dtime", "period", "cen_cost", "dtime_utc", "ckoeb_cost", "period_utc",
            "ceb_pp_cost", "ceb_sr_cost", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "crb_rozl_data"
    },
    "kmb-kro-rozl": {
        "columns": [
            "kmb", "kro", "dtime", "period", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "table": "kmb_kro_rozl_data"
    }
}


@task
def create_entity_table_if_not_exist(entity: str):
    config = ENTITY_CONFIG[entity]
    columns_sql = ",\n".join([f'"{col}" TEXT' for col in config["columns"]])
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS airflow_data."{config['table']}" (
        id SERIAL PRIMARY KEY,
        {columns_sql},
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_hook.run(create_sql)


@task
def fetch_entity_data(entity: str, execution_date=None) -> dict:
    date_str = execution_date.strftime("%Y-%m-%d")
    endpoint = f"/api/{entity}?$filter=business_date eq '{date_str}'"

    hook = HttpHook(method="GET", http_conn_id=HTTP_CONN_ID)
    session = hook.get_conn()
    response = session.get(hook.base_url.rstrip("/") + endpoint)
    response.raise_for_status()

    data = response.json().get("value", [])
    if not data:
        raise ValueError(f"No data returned for {entity} on {date_str}")

    df = pd.DataFrame(data)
    return {
        "entity": entity,
        "table": ENTITY_CONFIG[entity]["table"],
        "columns": ENTITY_CONFIG[entity]["columns"],
        "df": df.to_dict(orient="records")
    }


@task
def load_entity_data_to_postgres(data_dict: dict):
    entity = data_dict["entity"]
    table = data_dict["table"]
    columns = data_dict["columns"]
    df = pd.DataFrame(data_dict["df"])[columns]

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    conn = pg_hook.get_conn()
    cur = conn.cursor()
    columns_sql = ', '.join([f'"{col}"' for col in columns])
    cur.copy_expert(
        f"""COPY airflow_data."{table}" ({columns_sql}) FROM STDIN WITH CSV HEADER""",
        file=buffer
    )
    conn.commit()
    cur.close()
    conn.close()


@dag(
    dag_id="pse_multi_entity_etl",
    start_date=HISTORICAL_START_DATE,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["pse", "api", "etl"]
)
def pse_multi_entity_etl():
    for entity in ENTITY_CONFIG.keys():
        table_created = create_entity_table_if_not_exist.override(task_id=f"create_table_{entity}")(entity)
        raw = fetch_entity_data.override(task_id=f"fetch_data_{entity}")(entity)
        raw.set_upstream(table_created)
        load_entity_data_to_postgres.override(task_id=f"load_data_{entity}")(raw)


dag = pse_multi_entity_etl()
