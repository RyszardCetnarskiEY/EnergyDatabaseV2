import json
import os
import pickle
import shutil
import sys

from pendulum import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from tasks.df_processing_tasks import (
    add_timestamp_column,
    add_timestamp_elements,
    combine_df_and_params,
)
from tasks.entsoe_api_tasks import _generate_run_parameters_logic, extract_from_api
from tasks.sql_tasks import (
    cleanup_staging_tables,
    load_to_staging_table,
    merge_data_to_production,
)
from tasks.xml_processing_tasks import parse_xml

HISTORICAL_START_DATE = datetime(2025, 1, 1, tz="UTC")
HISTORICAL_END_DATE = datetime(2025, 1, 2, tz="UTC")


POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"  # Changed name for clarity

COUNTRY_MAPPING = {
    "PL": {"name": "Poland", "country_domain": ["10YPL-AREA-----S"], "bidding_zones": ["10YPL-AREA-----S"]},
    "DE": {
        "name": "Germany",
        "country_domain": ["10Y1001A1001A82H"],
        "bidding_zones": ["10YDE-VE-------2", "10YDE-EON------1", "10YDE-ENBW-----N", "10YDE-RWENET---I"],
    },
}


test_context = {}
test_context["logical_date"] = HISTORICAL_START_DATE  # or data_interval_start
test_context["data_interval_start"] = HISTORICAL_START_DATE  # or data_interval_start
test_context["data_interval_end"] = HISTORICAL_END_DATE  # or data_interval_start
test_context["dag_run"] = type("MockDagRun", (), {"run_id": "test_1234"})()

# [2025-06-03, 21:08:41 UTC] {entsoe_ingest.py:240} ERROR - No PT60M resolution data for Actual Generation per Generation Unit Poland {"securityToken": "ff91d43d-322a-43ba-b774-1f24d096388b", "periodStart": "202501010000", "periodEnd": "202501020000", "in_Domain": "10YPL-AREA-----S", "documentType": "A73", "processType": "A16"}

test_params = _generate_run_parameters_logic(HISTORICAL_START_DATE, HISTORICAL_END_DATE)
# test_param = test_params[4]
dirpath = os.path.join(os.path.join(os.path.dirname(__file__), "test_data"))
if not os.path.exists(dirpath):
    os.makedirs(dirpath)

i = 0
for test_param in test_params:
    test_case_name = f"{test_param["entsoe_api_params"]["periodStart"]}_{test_param["task_run_metadata"]["var_name"]}_{test_param["task_run_metadata"]["country_code"]}"
    case_path = os.path.join(os.path.join(dirpath, test_case_name))

    if os.path.exists(case_path):
        shutil.rmtree(case_path)
    os.makedirs(case_path)

    with open(os.path.join(case_path, "input_params.json"), "w") as outfile:
        json.dump(test_param, outfile)

    response = extract_from_api.function(test_param, **test_context)
    with open(os.path.join(case_path, "whole_response_dict.pkl"), "wb") as file:
        pickle.dump(response, file)
    with open(os.path.join(case_path, "entsoe_xml_response.xml"), "w") as file:
        file.write(response["xml_content"])

    df = parse_xml.function(response)
    df.to_csv(os.path.join(case_path, "df_no_timestamp.csv"))

    df_stamped = add_timestamp_column.function(df)
    df_stamped.to_csv(os.path.join(case_path, "df_timestamp.csv"))

    enriched_df = add_timestamp_elements.function(df_stamped)
    enriched_df.to_csv(os.path.join(case_path, "df_timestamp_elements.csv"))

    combined_for_staging = combine_df_and_params.function(df=enriched_df, task_param=test_param)
    staging_dict = load_to_staging_table.function(db_conn_id=POSTGRES_CONN_ID, df_and_params=combined_for_staging, **test_context)

    with open(os.path.join(case_path, "staging_results_dict.pkl"), "wb") as file:
        pickle.dump(staging_dict, file)

    merged_results = merge_data_to_production.function(db_conn_id=POSTGRES_CONN_ID, staging_dict=staging_dict)
    cleanup_staging_tables(db_conn_id=POSTGRES_CONN_ID, staging_dict=staging_dict)

    i += 1
