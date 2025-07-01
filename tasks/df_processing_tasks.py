import logging
from datetime import timedelta
from typing import Any, Dict

import pandas as pd
from airflow.decorators import task

logger = logging.getLogger(__name__)
POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"  # Changed name for clarity


@task(task_id='add_timestamp')
def add_timestamp_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a timestamp column to the DataFrame based on Period_Start, Resolution, and index within TimeSeries_ID.
    Assumes Resolution is always PT60M (1 hour) for simplicity.
    """

    if df.empty:
        logger.info("Skipping timestamp addition for empty DataFrame.")
        return df.assign(timestamp=pd.NaT)

    df = df.copy()
    if 'Period_Start' not in df.columns or df['Period_Start'].isnull().all():
        logger.warning("Missing 'Period_Start' column or all values are null. Cannot add timestamp.")
        df['timestamp'] = pd.NaT
        return df.drop(columns=[col for col in ['Period_Start_dt', 'Resolution_td', 'Period_Start', 'Period_End'] if col in df.columns], errors='ignore')

    df['Period_Start_dt'] = pd.to_datetime(df['Period_Start'], utc=True, errors='coerce')

    def parse_resolution(res):
        if res == 'PT60M': return timedelta(hours=1)
        if res == 'PT30M': return timedelta(minutes=30)
        if res == 'PT15M': return timedelta(minutes=15)
        logger.warning(f"Unsupported resolution: {res}, will result in NaT timestamp for affected rows.")
        return pd.NaT

    df['Resolution_td'] = df['Resolution'].apply(parse_resolution)
    df['Position'] = pd.to_numeric(df['Position'], errors='coerce').fillna(0).astype(int)
    df['timestamp'] = df['Period_Start_dt'] + (df['Position'] - 1) * df['Resolution_td']
    df.drop(columns=['Period_Start_dt', 'Resolution_td', 'Period_Start', 'Period_End'], inplace=True, errors='ignore')
    return df

@task(task_id='add_timestamp_elements')
def add_timestamp_elements(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or 'timestamp' not in df.columns or df['timestamp'].isnull().all():
        logger.info("Skipping timestamp element addition for empty or invalid DataFrame.")

        for col in ['year', 'quarter', 'month', 'day', 'dayofweek', 'hour']:
            if col not in df.columns: df[col] = pd.NA 
        return df

    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    
    valid_timestamps = df['timestamp'].notna()
    df.loc[valid_timestamps, 'year'] = df.loc[valid_timestamps, 'timestamp'].dt.year.astype(int)
    df.loc[valid_timestamps, 'quarter'] = df.loc[valid_timestamps, 'timestamp'].dt.quarter.astype(int)
    df.loc[valid_timestamps, 'month'] = df.loc[valid_timestamps, 'timestamp'].dt.month.astype(int)
    df.loc[valid_timestamps, 'day'] = df.loc[valid_timestamps, 'timestamp'].dt.day.astype(int)
    df.loc[valid_timestamps, 'dayofweek'] = df.loc[valid_timestamps, 'timestamp'].dt.dayofweek.astype(int) # Monday=0, Sunday=6
    df.loc[valid_timestamps, 'hour'] = df.loc[valid_timestamps, 'timestamp'].dt.hour.astype(int)

    return df


@task
def combine_df_and_params(df: pd.DataFrame, task_param: Dict[str, Any]):
    return {"df": df, "params": task_param}
