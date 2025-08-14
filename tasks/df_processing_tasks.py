import logging
from datetime import timedelta
from typing import Any, Dict

import pandas as pd
from airflow.decorators import task

logger = logging.getLogger(__name__)
POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"  # Changed name for clarity


@task(task_id='add_timestamp')
def add_timestamp_column(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not parsed_data.get("success", False):
            #return {**parsed_data, "success": False, "error": parsed_data.get("error", "Upstream error")}
            return {**parsed_data, "success": False, "error": parsed_data.get("error", "Upstream error"), "df": pd.DataFrame()}

        df = parsed_data.get("df", pd.DataFrame())
        if df.empty:
            #return {**parsed_data, "success": False, "error": "Empty DataFrame after parse_xml"}
            return {**parsed_data, "success": False, "error": "Empty DataFrame after parse_xml", "df": pd.DataFrame()}

        df = df.copy()
        df['Period_Start_dt'] = pd.to_datetime(df['Period_Start'], utc=True, errors='coerce')

        def parse_resolution(res):
            if res == 'PT60M': return timedelta(hours=1)
            if res == 'PT30M': return timedelta(minutes=30)
            if res == 'PT15M': return timedelta(minutes=15)
            return pd.NaT

        df['Resolution_td'] = df['Resolution'].apply(parse_resolution)
        df['Position'] = pd.to_numeric(df['Position'], errors='coerce').fillna(0).astype(int)
        df['timestamp'] = df['Period_Start_dt'] + (df['Position'] - 1) * df['Resolution_td']
        df.drop(columns=['Period_Start_dt', 'Resolution_td', 'Period_Start', 'Period_End'], inplace=True, errors='ignore')

        return {**parsed_data, "df": df, "success": True}
    except Exception as e:
        logger.error(f"[add_timestamp_column] Error: {str(e)}")
        #return {**parsed_data, "success": False, "error": str(e)}
        return {**parsed_data, "success": False, "error": str(e), "df": pd.DataFrame()}

@task(task_id='add_timestamp_elements')
def add_timestamp_elements(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not parsed_data.get("success", False):
            #return {**parsed_data, "success": False}
            return {**parsed_data, "success": False, "df": pd.DataFrame()}

        df = parsed_data.get("df", pd.DataFrame())
        if df.empty or "timestamp" not in df.columns:
            #return {**parsed_data, "success": False, "error": "Missing timestamp"}
            return {**parsed_data, "success": False, "error": "Missing timestamp", "df": pd.DataFrame()}

        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        valid = df['timestamp'].notna()

        df.loc[valid, 'year'] = df.loc[valid, 'timestamp'].dt.year.astype(int)
        df.loc[valid, 'quarter'] = df.loc[valid, 'timestamp'].dt.quarter.astype(int)
        df.loc[valid, 'month'] = df.loc[valid, 'timestamp'].dt.month.astype(int)
        df.loc[valid, 'day'] = df.loc[valid, 'timestamp'].dt.day.astype(int)
        df.loc[valid, 'dayofweek'] = df.loc[valid, 'timestamp'].dt.dayofweek.astype(int)
        df.loc[valid, 'hour'] = df.loc[valid, 'timestamp'].dt.hour.astype(int)

        return {**parsed_data, "df": df, "success": True}
    except Exception as e:
        logger.error(f"[add_timestamp_elements] Error: {str(e)}")
        #return {**parsed_data, "success": False, "error": str(e)}
        return {**parsed_data, "success": False, "error": str(e), "df": pd.DataFrame()}

@task
def combine_df_and_params(df: pd.DataFrame, task_param: Dict[str, Any]) -> Dict[str, Any]:
    try:
        logger.info("[combine_df_and_params] df_shape=%s, var=%s, area=%s",
                    df.shape if isinstance(df, pd.DataFrame) else None,
                    task_param.get("task_run_metadata", {}).get("var_name"),
                    task_param.get("task_run_metadata", {}).get("area_code"))
        return {"df": df, "task_param": task_param, "success": not df.empty}
    except Exception as e:
        logger.exception("[combine_df_and_params] error")
        return {"df": pd.DataFrame(), "task_param": task_param, "success": False, "error": str(e)}
