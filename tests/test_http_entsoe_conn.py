import json
import os
import pickle
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pendulum import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

from dags.entsoe_ingest import extract_from_api

HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")
HISTORICAL_END_DATE = datetime(2025, 1, 2, tz="UTC")

POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"  # Changed name for clarity

TEST_CASES_DIR = os.path.join(os.path.join(os.path.dirname(__file__), "test_data"))
TEST_CASES = [p.name for p in Path(TEST_CASES_DIR).iterdir() if p.is_dir()]


def load_xml_test_data(case_name):
    case_dir = os.path.join(TEST_CASES_DIR, case_name)
    input_data = json.load(open(os.path.join(case_dir, "input_params.json")))

    with open(os.path.join(case_dir, "entsoe_xml_response.xml"), "r") as file:
        expected_xml_output = file.read()

    expected_result_dict = pickle.load(open(os.path.join(case_dir, "whole_response_dict.pkl"), "rb"))

    return input_data, expected_xml_output, expected_result_dict


@pytest.fixture()
def airflow_context():
    test_context = {}
    test_context["logical_date"] = HISTORICAL_START_DATE  # or data_interval_start
    test_context["data_interval_start"] = HISTORICAL_START_DATE  # or data_interval_start
    test_context["data_interval_end"] = HISTORICAL_END_DATE  # or data_interval_start
    test_context["dag_run"] = type("MockDagRun", (), {"run_id": "test_1234"})()
    return test_context


@patch("tasks.entsoe_api_tasks.HttpHook")
def test_http_connection(mock_http_hook_class):
    mock_http_hook = MagicMock()
    mock_conn = MagicMock()

    mock_conn.password = "dummy_token"
    mock_conn.host = "web-api.tp.entsoe.eu"

    mock_http_hook.get_connection.return_value = mock_conn
    mock_http_hook_class.return_value = mock_http_hook

    from tasks.entsoe_api_tasks import _entsoe_http_connection_setup

    base_url, api_key, conn, http_hook = _entsoe_http_connection_setup()
    assert len(set(api_key)) > 5

    assert api_key.strip() == api_key
    assert base_url == "https://web-api.tp.entsoe.eu/api"


@pytest.mark.parametrize("case_name", TEST_CASES)
@patch("tasks.entsoe_api_tasks.HttpHook")
def test_extract_from_api(mock_http_hook_class, case_name, airflow_context):

    input_data, expected_xml, expected_result_dict = load_xml_test_data(case_name)

    mock_http_hook = MagicMock()
    mock_response = MagicMock()

    mock_response.status_code = 200
    mock_response.text = expected_xml
    mock_response.headers.get.return_value = "application/xml"

    mock_http_hook.get_connection.return_value = MagicMock(password="fake_token", host="web-api.tp.entsoe.eu/api")
    mock_http_hook.get_conn.return_value.get.return_value = mock_response
    mock_http_hook_class.return_value = mock_http_hook

    result = extract_from_api.function(input_data, **airflow_context)

    assert result["status_code"] == 200
    assert "xml_content" in result
    assert "<TimeSeries>" in expected_result_dict["xml_content"]
    assert result["country_name"] == expected_result_dict["task_run_metadata"]["country_name"]
    assert result["var_name"] == expected_result_dict["task_run_metadata"]["var_name"]
    assert result["area_code"] == input_data["entsoe_api_params"]["in_Domain"]
    assert result["period_start"] == input_data["entsoe_api_params"]["periodStart"]
    assert result["period_end"] == input_data["entsoe_api_params"]["periodEnd"]
