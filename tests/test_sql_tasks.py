import json
import os
import pickle
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pendulum import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from tasks.sql_tasks import (
    _create_prod_table,
    _create_table_columns,
    cleanup_staging_tables,
    create_initial_tables,
    load_to_staging_table,
    merge_data_to_production,
)

HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")
HISTORICAL_END_DATE = datetime(2025, 1, 2, tz="UTC")


TEST_CASES_DIR = os.path.join(os.path.join(os.path.dirname(__file__), "test_data"))
TEST_CASES = [p.name for p in Path(TEST_CASES_DIR).iterdir() if p.is_dir()]


def load_df_test_data(case_name):
    case_dir = os.path.join(TEST_CASES_DIR, case_name)
    input_data = json.load(open(os.path.join(case_dir, "input_params.json")))

    parsed_df_timestamp_elements = pd.read_csv(os.path.join(case_dir, "df_timestamp_elements.csv"), index_col=0)

    staging_results_dict = pickle.load(open(os.path.join(case_dir, "staging_results_dict.pkl"), "rb"))

    df_and_params = {"df": parsed_df_timestamp_elements, "params": input_data}

    return input_data, parsed_df_timestamp_elements, staging_results_dict, df_and_params


@pytest.fixture
def mock_postgres_hook():
    hook = MagicMock()
    conn = MagicMock()
    cursor = MagicMock()
    hook.get_conn.return_value = conn
    conn.cursor.return_value = cursor
    return hook, conn, cursor


@pytest.fixture
def expected_columns():
    return [
        '"Resolution" TEXT',
        '"quantity" NUMERIC',
        '"variable" TEXT',
        '"area_code" TEXT',
        '"timestamp" TEXT',
        '"year" NUMERIC',
        '"quarter" NUMERIC',
        '"month" NUMERIC',
        '"day" NUMERIC',
        '"dayofweek" NUMERIC',
        '"hour" NUMERIC',
    ]


@pytest.mark.parametrize("case_name", TEST_CASES)
def test_create_table_columns(case_name, expected_columns):
    _, parsed_df_timestamp_elements, _, _ = load_df_test_data(case_name)

    df = parsed_df_timestamp_elements
    df = df.drop("Position", axis=1)  # Todo, a little ugly but not sure how to fix now

    assert _create_table_columns(df) == expected_columns, repr(_create_table_columns(df))


@pytest.mark.parametrize("case_name", TEST_CASES)
@patch("tasks.sql_tasks.random.randint", return_value=12345)
@patch("tasks.sql_tasks.PostgresHook")
def test_load_to_staging_table_with_data(
    mock_postgres_hook_class,
    mock_randint,
    case_name,
    mock_postgres_hook,
    expected_columns,
):
    hook, conn, cursor = mock_postgres_hook
    mock_postgres_hook_class.return_value = hook

    task_param, _, _, df_and_params = load_df_test_data(case_name)

    result = load_to_staging_table.function(df_and_params)

    random_number = 12345

    expected_table = f"stg_entsoe_{task_param['task_run_metadata']['country_code']}_{task_param['entsoe_api_params']['periodStart']}_{random_number}"
    assert result["staging_table_name"].startswith(expected_table[:63]), f"Expected table name {result['staging_table_name']}"
    assert result["var_name"] == task_param["task_run_metadata"]["var_name"], f"Expected var name {result['var_name']}"
    assert hook.run.call_count >= 2, hook.run.call_count

    hook.run.assert_any_call(
        f'CREATE TABLE airflow_data."{expected_table}" (id SERIAL PRIMARY KEY, {", ".join(expected_columns)}, processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);'
    )
    hook.run.assert_any_call(f'DROP TABLE IF EXISTS airflow_data."{expected_table}";')
    assert hook.run.call_count == 2, hook.run.call_count

    assert cursor.copy_expert.called, cursor.copy_expert.called


@pytest.mark.parametrize("case_name", TEST_CASES)
@patch("tasks.sql_tasks._create_prod_table")
@patch("tasks.sql_tasks.PostgresHook")
def test_merge_executes_sql(mock_pg_hook_class, mock_create_prod_table, case_name):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    input_data, parsed_df_timestamp_elements, staging_results_dict, df_and_params = load_df_test_data(case_name)

    result = merge_data_to_production.function(staging_results_dict, db_conn_id="fake_conn")

    production_table = staging_results_dict["var_name"].replace(" ", "_").lower()
    staging_table = staging_results_dict["staging_table_name"]

    assert result == f"Merged {staging_table}", f"expected {staging_table} actual {result}"
    mock_create_prod_table.assert_called_once_with(production_table)

    assert mock_pg_hook.run.call_count == 1
    actual_sql = mock_pg_hook.run.call_args[0][0]
    assert f'FROM airflow_data."{staging_table}"' in actual_sql
    assert f'INTO airflow_data."{production_table}"' in actual_sql, actual_sql


@patch("tasks.sql_tasks.PostgresHook")
@patch("tasks.sql_tasks.logger")
def test_create_initial_tables(mock_logger, mock_pg_hook_class):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    result = create_initial_tables.function("my_conn_id", "test_raw_table")

    expected_sql_snippet = 'CREATE TABLE IF NOT EXISTS airflow_data."test_raw_table"'
    actual_sql = mock_pg_hook.run.call_args[0][0]

    assert expected_sql_snippet in actual_sql
    mock_pg_hook.run.assert_called_once()
    assert result == {"raw_xml_table": "test_raw_table"}
    mock_logger.info.assert_called_once_with('Ensured raw XML table airflow_data."test_raw_table" exists.')


@patch("tasks.sql_tasks.PostgresHook")
@patch("tasks.sql_tasks.logger")
def test_create_prod_table(mock_logger, mock_pg_hook_class):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    _create_prod_table("my_var")

    expected_sql_snippet = 'CREATE TABLE IF NOT EXISTS airflow_data."my_var"'
    actual_sql = mock_pg_hook.run.call_args[0][0]

    assert expected_sql_snippet in actual_sql
    mock_pg_hook.run.assert_called_once()
    mock_logger.info.assert_called_once_with('Ensured production table airflow_data."my_var" exists.')


@patch("tasks.sql_tasks.PostgresHook")
@patch("tasks.sql_tasks.logger")
def test_cleanup_staging_tables_drops_table(mock_logger, mock_pg_hook_class):
    mock_pg_hook = MagicMock()
    mock_pg_hook_class.return_value = mock_pg_hook

    staging_dict = {"staging_table_name": "stg_table_x"}
    cleanup_staging_tables.function(staging_dict, "fake_conn")

    mock_pg_hook.run.assert_called_once_with('DROP TABLE IF EXISTS airflow_data."stg_table_x";')
    mock_logger.info.assert_called_once_with('Dropped staging table: airflow_data."stg_table_x".')
