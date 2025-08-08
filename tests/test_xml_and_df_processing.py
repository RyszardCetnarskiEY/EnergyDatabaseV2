import os
import pickle
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from dags.entsoe_ingest import parse_xml, store_raw_xml
from tasks.df_processing_tasks import add_timestamp_column, add_timestamp_elements

TEST_CASES_DIR = os.path.join(os.path.join(os.path.dirname(__file__), "test_data"))
TEST_CASES = [p.name for p in Path(TEST_CASES_DIR).iterdir() if p.is_dir()]


def load_df_test_data(case_name):
    case_dir = os.path.join(TEST_CASES_DIR, case_name)
    # input_data = json.load(open(os.path.join(case_dir, "input_params.json")))

    df_no_timestamp = pd.read_csv(os.path.join(case_dir, "df_no_timestamp.csv"), index_col=0)
    df_timestamp = pd.read_csv(os.path.join(case_dir, "df_timestamp.csv"), index_col=0)
    df_timestamp_elements = pd.read_csv(os.path.join(case_dir, "df_timestamp_elements.csv"), index_col=0)

    whole_response = pickle.load(open(os.path.join(case_dir, "whole_response_dict.pkl"), "rb"))
    whole_response
    return df_no_timestamp, df_timestamp, df_timestamp_elements, whole_response


@pytest.mark.parametrize("case_name", TEST_CASES)
def test_parse_xml(case_name):
    df_no_timestamp, df_timestamp, df_timestamp_elements, whole_response = load_df_test_data(case_name)
    parsed = parse_xml.function(whole_response)
    parsed_df = parsed["df"]
    pd.testing.assert_frame_equal(parsed_df, df_no_timestamp)
    assert parsed["success"] is True


@pytest.mark.parametrize("case_name", TEST_CASES)
def test_add_timestamp_column(case_name):
    df_no_timestamp, df_timestamp, df_timestamp_elements, whole_response = load_df_test_data(case_name)
    parsed_timestamp_df = add_timestamp_column.function({"success" :True, 'df' : df_no_timestamp})["df"]
    assert parsed_timestamp_df is not None
    assert "timestamp" in parsed_timestamp_df.columns
    assert all(parsed_timestamp_df["timestamp"] == df_timestamp["timestamp"])


@pytest.mark.parametrize("case_name", TEST_CASES)
def test_add_timestamp_elements(case_name):
    df_no_timestamp, df_timestamp, df_timestamp_elements, whole_response = load_df_test_data(case_name)
    parsed_timestamp_elements_df = add_timestamp_elements.function({"success" :True, 'df' : df_timestamp})["df"]
    # print(list(parsed_timestamp_elements_df["year"].unique()))
    assert set(parsed_timestamp_elements_df["year"].unique()).issubset(set([2024, 2025]))
    assert set(parsed_timestamp_elements_df["month"].unique()).issubset(set([12, 1]))
    assert set(parsed_timestamp_elements_df["quarter"].unique()).issubset(set([4, 1]))
    assert set(parsed_timestamp_elements_df["day"].unique()).issubset(set([31, 1, 2]))
    assert set(parsed_timestamp_elements_df["dayofweek"].unique()).issubset(
        set(
            [
                1,
                2,
                3,
            ]
        )
    ), parsed_timestamp_elements_df["dayofweek"].unique()

    assert parsed_timestamp_elements_df["hour"].min() == 0
    assert parsed_timestamp_elements_df["hour"].max() == 23


@pytest.mark.parametrize("case_name", TEST_CASES)
@patch("tasks.xml_processing_tasks.PostgresHook")
def test_store_raw_xml_success(mock_pg_hook_class, case_name):
    mock_pg_hook = MagicMock()
    mock_pg_hook.run.return_value = 42
    mock_pg_hook_class.return_value = mock_pg_hook
    df_no_timestamp, df_timestamp, df_timestamp_elements, whole_response = load_df_test_data(case_name)

    result = store_raw_xml.function(whole_response, "test_conn", "test_table")

    assert result['raw_id'] == 42
    mock_pg_hook.run.assert_called_once()
    args, kwargs = mock_pg_hook.run.call_args
    assert "INSERT INTO airflow_data" in args[0]  # Check SQL string
    assert kwargs["parameters"] == (
        whole_response["var_name"],
        whole_response["country_name"],
        whole_response["area_code"],
        whole_response["status_code"],
        whole_response["period_start"],
        whole_response["period_end"],
        whole_response["logical_date_processed"],
        whole_response["xml_content"],
        whole_response["request_params"],
        whole_response["content_type"],
    )
