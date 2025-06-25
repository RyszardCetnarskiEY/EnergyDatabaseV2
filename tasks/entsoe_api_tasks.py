import json
import logging
import os
import sys
from typing import Any, Dict, List

import requests
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from tasks.entsoe_dag_config import COUNTRY_MAPPING, ENTSOE_VARIABLES

logger = logging.getLogger(__name__)
POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"  # Changed name for clarity


def _generate_run_parameters_logic(data_interval_start, data_interval_end):
    """Core logic extracted for testing"""

    task_params = []

    for var_name, entsoe_params_dict in ENTSOE_VARIABLES.items():
        for country_code, country_details in COUNTRY_MAPPING.items():
            for in_domain in country_details[entsoe_params_dict["AreaType"]]:
                task_params.append(
                    {
                        "entsoe_api_params": {
                            "periodStart": data_interval_start.strftime("%Y%m%d%H%M"),
                            "periodEnd": data_interval_end.strftime("%Y%m%d%H%M"),
                            "in_Domain": in_domain,
                            **entsoe_params_dict["params"],
                        },
                        "task_run_metadata": {
                            "var_name": var_name,
                            "config_dict": entsoe_params_dict,
                            "country_code": country_code,
                            "country_name": country_details["name"],
                        },
                    }
                )

    return task_params


@task
def generate_run_parameters(**context) -> List[Dict[str, str]]:
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    task_params = _generate_run_parameters_logic(data_interval_start, data_interval_end)

    logger.info(
        f"Generated {len(task_params)} parameter sets for data interval: {data_interval_start.to_date_string()} - {data_interval_end.to_date_string()}"
    )
    return task_params


def _entsoe_http_connection_setup():
    http_hook = HttpHook(method="GET", http_conn_id="ENTSOE")
    conn = http_hook.get_connection("ENTSOE")
    api_key = conn.password

    logger.info(f"securityToken: {api_key[0:10]!r}")  # the !r will show hidden chars
    # logger.info(f"securityToken: {api_key[10::]!r}")  # the !r will show hidden chars

    base_url = conn.host.rstrip("/")  # rstrip chyba nic nie robi, do usunięcia
    # Todo, do I need to define this connection anew every task instance?
    if conn.host.startswith("http"):
        base_url = conn.host.rstrip("/")
    else:
        base_url = (
            f"https://{conn.host.rstrip('/')}" + "/api"
        )  # Added because I think airflow UI connections and one defined in helm chart behave slightly differently

    logger.info(f"[DEBUG] REQUEST URL: {base_url}")

    return base_url, api_key, conn, http_hook


def _get_entsoe_response(log_str, api_request_params):
    logger.info(f"Fetching data for {log_str}")
    base_url, api_key, conn, http_hook = _entsoe_http_connection_setup()

    api_request_params = {
        "securityToken": api_key,
        **api_request_params,
    }
    session = http_hook.get_conn()
    response = session.get(base_url, params=api_request_params, timeout=60)

    response.raise_for_status()
    response.encoding = "utf-8"  # explicitly set encoding if not set
    return response


@task(task_id="extract_from_api")
def extract_from_api(task_param: Dict[str, Any], **context) -> Dict[str, Any]:
    """
    Fetches data from the ENTSOE API for a given country and period.
    api_params is expected to be a dict with 'periodStart', 'periodEnd', 'country_code'.
    """
    entsoe_api_params = task_param["entsoe_api_params"]
    task_run_metadata = task_param["task_run_metadata"]
    api_kwargs = {}
    if task_run_metadata["var_name"] == "Energy Prices fixing I":
        api_kwargs["out_Domain"] = entsoe_api_params["in_Domain"]

    api_request_params = dict(entsoe_api_params, **api_kwargs)

    log_str = (
        f"{task_run_metadata['var_name']} {task_run_metadata['country_name']} "
        f"({entsoe_api_params['in_Domain']}) for period: {entsoe_api_params['periodStart']} - {entsoe_api_params['periodEnd']}"
    )

    try:
        response = _get_entsoe_response(log_str, api_request_params)
        logger.info(f"Successfully extracted data for  {log_str}")

        return {
            "xml_content": response.text,
            # 'xml_bytes': response.content, #TODO a bit heavy, choose one, content or text
            "status_code": response.status_code,
            "content_type": response.headers.get("Content-Type"),
            "var_name": task_run_metadata["var_name"],
            "country_name": task_run_metadata["country_name"],
            "country_code": task_run_metadata[
                "country_code"
            ],  # Todo, trochę za dużo tych nazw dla kraju, może już kod niepotrzebny?
            "area_code": entsoe_api_params["in_Domain"],
            "period_start": entsoe_api_params["periodStart"],
            "period_end": entsoe_api_params["periodEnd"],
            "logical_date_processed": context["logical_date"].isoformat(),  # or data_interval_start
            "request_params": json.dumps(api_request_params),
            "task_run_metadata": task_run_metadata,
        }

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error for {log_str}: {http_err} - Response: {response.text}")
        raise AirflowException(
            f"API request failed for {log_str} with status {response.status_code}: {response.text}"
        )
    except Exception as e:
        logger.error(f"Error extracting data for {log_str}: {str(e)}")
        raise
