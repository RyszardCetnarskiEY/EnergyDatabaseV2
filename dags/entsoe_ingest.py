import os
import sys
from datetime import timedelta

from airflow.decorators import dag, task
from pendulum import datetime

# Add the project root directory to sys.path so imports work from dags/
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tasks.df_processing_tasks import (
    add_timestamp_column,
    add_timestamp_elements,
    combine_df_and_params,
)
from tasks.entsoe_api_tasks import extract_from_api, generate_run_parameters
from tasks.entsoe_dag_config import POSTGRES_CONN_ID, RAW_XML_TABLE_NAME
from tasks.sql_tasks import (
    cleanup_staging_tables,
    create_initial_tables,
    load_to_staging_table,
    merge_data_to_production,
)
from tasks.xml_processing_tasks import parse_xml, store_raw_xml

HISTORICAL_START_DATE = datetime(2022, 12, 19, tz="UTC")

# logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task
def zip_df_and_params(
    dfs: list,
    params: list,
) -> list[dict]:
    if len(dfs) != len(params):
        raise ValueError(f"Cannot zip: len(dfs)={len(dfs)} vs len(params)={len(params)}")
    return [{"df": df_obj, "task_param": param} for df_obj, param in zip(dfs, params)]


print(
    "TODO - move to taTODO - move to taskGroup one day and share some tasks for other variables, like generating units operation pointskGroup one day and share some tasks for other variables, like generating units operation points"
)
# A more advanced pattern could be a TaskGroup that is mapped.


@dag(
    dag_id="entsoe_dynamic_etl_pipeline_final",
    default_args=default_args,
    description="Daily ETL for ENTSO-E day-ahead prices for multiple countries since 2023-01-01.",
    schedule="@daily",
    start_date=HISTORICAL_START_DATE,  # CRITICAL: Use timezone-aware datetime
    catchup=True,
    tags=["entsoe", "energy", "api", "etl", "dynamic"],
    max_active_runs=1,  # Limit to 1 active DAG run to avoid overwhelming API/DB during backfill
    doc_md=__doc__,
)
def entsoe_dynamic_etl_pipeline():
    # --- Task Orchestration ---

    initial_setup = create_initial_tables(db_conn_id=POSTGRES_CONN_ID, raw_xml_table=RAW_XML_TABLE_NAME)

    task_parameters = generate_run_parameters()

    # Ensure setup is done before dynamic tasks
    task_parameters.set_upstream(initial_setup)

    extracted_data = extract_from_api.expand(task_param=task_parameters)

    stored_xml_ids = store_raw_xml.partial(
        db_conn_id=POSTGRES_CONN_ID,
        table_name=initial_setup["raw_xml_table"],  # Use XComArg from setup task
    ).expand(extracted_data=extracted_data)

    parsed_dfs = parse_xml.expand(extracted_data=extracted_data)

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

    merged_results = merge_data_to_production.partial(db_conn_id=POSTGRES_CONN_ID).expand(
        staging_dict=staging_dict
    )

    cleanup_task = cleanup_staging_tables.partial(db_conn_id=POSTGRES_CONN_ID).expand(staging_dict=staging_dict)

    # Set cleanup to run after all merges
    cleanup_task.set_upstream(merged_results)

    # Explicitly make sure XML storage is upstream of parsing (though usually inferred)
    parsed_dfs.set_upstream(stored_xml_ids)


entsoe_dynamic_etl_dag = entsoe_dynamic_etl_pipeline()
