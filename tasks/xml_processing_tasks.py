from datetime import timedelta
from pendulum import datetime
import json
import logging
import requests
import pandas as pd
from typing import Dict, Any, List
import xml.etree.ElementTree as ET
from io import StringIO
import debugpy

from airflow.operators.python import get_current_context

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook

from airflow.operators.empty import EmptyOperator # Not used in final version, but good to know
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago # Alternative for start_date

import sys
import os

import logging

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing" # Changed name for clarity


@task
def store_raw_xml(extracted_data: Dict[str, Any], db_conn_id: str, table_name: str) -> int:

    if not extracted_data or 'xml_content' not in extracted_data:
        logger.warning(f"Skipping storage of raw XML due to missing content for params: {extracted_data.get('request_params')}")
        return -1
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    # debugpy.listen(("0.0.0.0", 8508))
    # debugpy.wait_for_client()
    # debugpy.breakpoint()
    sql = f"""
    INSERT INTO airflow_data."{table_name}"
        (var_name, country_name, area_code, status_code, period_start, period_end, request_time, xml_data, request_parameters, content_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    RETURNING id;"""
    try:
        inserted_id = pg_hook.run(
            sql,
            parameters=(
                extracted_data['var_name'],
                extracted_data['country_name'],
                extracted_data['area_code'],
                extracted_data['status_code'],
                extracted_data['period_start'],
                extracted_data['period_end'],
                extracted_data['logical_date_processed'],
                extracted_data['xml_content'],
                extracted_data['request_params'],
                extracted_data['content_type'],
            ),
            handler=lambda cursor: cursor.fetchone()[0]
        )
        logger.info(f"Stored raw XML with ID {inserted_id} for request targeting {extracted_data.get('country_name')}")
        return inserted_id
    except Exception as e:
        logger.error(f"Error storing raw XML for {extracted_data.get('country_name')}: {e}")
        raise

@task(task_id='parse_xml')
def parse_xml(extracted_data: Dict[str, Any]) -> pd.DataFrame:

    xml_data = extracted_data['xml_content']
    country_name = extracted_data['country_name']
    area_code = extracted_data['area_code']

    var_name = extracted_data['var_name']
    column_name = extracted_data['task_run_metadata']['config_dict']["xml_parsing_info"]['column_name']
    var_resolution = extracted_data['task_run_metadata']['config_dict']["xml_parsing_info"]['resolution']
    request_params_str = extracted_data['request_params']

    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        raise ValueError(f"Invalid XML format: {e}")

    # Pretty-print the XML
    import xml.dom.minidom
    dom = xml.dom.minidom.parseString(xml_data)
    pretty_xml = dom.toprettyxml(indent="  ")

    # Determine namespace
    ns = {'ns': root.tag[root.tag.find("{")+1:root.tag.find("}")]} if "{" in root.tag else {}
    print('ns: ', ns)

    max_pos = 0

    #results_name_dict = {}
    results_df = pd.DataFrame(columns = ["Position", "Period_Start", "Period_End", "Resolution", "quantity", "variable"])
    resolutions = [elem.text for elem in root.findall('.//ns:resolution', namespaces=ns)]
    found_res = var_resolution in resolutions
    for ts in root.findall('ns:TimeSeries', ns):
        # Determine column name
        name = ts.findtext(column_name, namespaces=ns)
        if var_name == "Actual Generation per Generation Unit":
            #TODO rozpracuj to na poziomie configa, może wystarczy wskazać mRID zamiast name. Albo w ogóle zawsze po mRID zapisywać i mieć foreign tables mapujące mRID na coś czytelnego
            name = ts.findtext('ns:MktPSRType/ns:PowerSystemResources/ns:mRID', namespaces=ns)#Nazwy bloków mogą być nieunikalne, użyj innego id
        if not name:
            raise ValueError(f"no data in xml for: {column_name}")
        name = name.strip()

        period = ts.find('ns:Period', ns)
        if period is None:
            logger.error(f"No  data for {var_name} {country_name} {request_params_str}")
            continue
        resolution = period.findtext('.//ns:resolution', namespaces=ns)
        if resolution != var_resolution and found_res:
            # Do not store values in two resolutions 
            continue

        timeInterval = period.findall('ns:timeInterval', ns)
        start = period.findtext('ns:timeInterval/ns:start', namespaces=ns)
        end = period.findtext('ns:timeInterval/ns:end', namespaces=ns)

        # Extract all <Point> values
        data = {}
        for point in period.findall('ns:Point', ns):
            pos = point.findtext('ns:position', namespaces=ns)
            qty = point.findtext('ns:quantity', namespaces=ns) or point.findtext('ns:price.amount', namespaces=ns) # TODO Also pass this in the xml parsing params like name
            if pos is not None and qty is not None:
                try:
                    pos = int(pos)
                    qty = float(qty)
                    data[pos] = qty
                    max_pos = max(max_pos, pos)
                except ValueError:
                    continue  # Skip malformed points
        
        if column_name == 'ns:mRID': 
            value_label = var_name
        else: 
            value_label = name

        # if value_label not in results_name_dict:
        #     results_name_dict[value_label] = pd.DataFrame(columns=[value_label, "Period_Start", "Period_End"])
        
        partial_df = pd.DataFrame.from_dict(data, orient='index', columns=["quantity"])
        #TODO: if constructing a multicolumn dataframe, there is no need to store period start and period end for each column. They will be exactly the same. Either drop duplicates or remove altogether
        partial_df.loc[:, "Period_Start"] = start
        partial_df.loc[:, "Period_End"] = end
        partial_df.loc[:, "Resolution"] = resolution #TODO - fix large and small letters...
        partial_df.loc[:, "variable"] = value_label
        results_df = pd.concat([results_df, partial_df.reset_index(drop=False, names = 'Position' )], axis=0)
        #results_name_dict[value_label] = pd.concat([results_name_dict[value_label], partial_df])

    if len(results_df)==0:
        raise ValueError("No valid TimeSeries data found in XML.")

    #df = pd.concat(results_name_dict, axis=1).T.drop_duplicates().T
    #df.columns = df.columns.droplevel()
    #if df.columns.duplicated().any():
    #    debugpy.listen(("0.0.0.0", 8508))
    #    debugpy.wait_for_client()  
    #    debugpy.breakpoint()


    #df = df.reset_index(drop=False, names = 'Position' )
    #df.loc[:,"country_name"] = country_name
    results_df.loc[:,"area_code"] = area_code
    return results_df