import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict

import pandas as pd
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"  # Changed name for clarity


@task(task_id='store_raw_xml')
def store_raw_xml(extracted_data: Dict[str, Any], db_conn_id: str, table_name: str) -> Dict[str, Any]:
    try:
        if not extracted_data.get("success", False):
            return {**extracted_data, "stored": False, "error": extracted_data.get("error", "extract failed")}

        pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
        sql = f"""
        INSERT INTO airflow_data."{table_name}"
            (var_name, country_name, area_code, status_code, period_start, period_end, request_time, xml_data, request_parameters, content_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        RETURNING id;"""

        inserted_id = pg_hook.run(
            sql,
            parameters=(
                extracted_data['task_param']['task_run_metadata']['var_name'],
                extracted_data['task_param']['task_run_metadata']['country_name'],
                extracted_data['task_param']['task_run_metadata']['area_code'],
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
        return {**extracted_data, "stored": True, "raw_id": inserted_id}
    except Exception as e:
        logger.error(f"[store_raw_xml] Error: {str(e)}")
        return {**extracted_data, "success": False, "stored": False, "error": str(e)}

@task(task_id='parse_xml')
def parse_xml(extracted_data: Dict[str, Any]) -> pd.DataFrame:
    try:
        if not extracted_data.get("success", False):
            return {**extracted_data, "success": False, "error": "Upstream failure in extract_from_api"}

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

        import xml.dom.minidom
        dom = xml.dom.minidom.parseString(xml_data)
        pretty_xml = dom.toprettyxml(indent="  ")

        ns = {'ns': root.tag[root.tag.find("{")+1:root.tag.find("}")]} if "{" in root.tag else {}
        print('ns: ', ns)

        max_pos = 0

        results_df = pd.DataFrame(columns = ["Position", "Period_Start", "Period_End", "Resolution", "quantity", "variable"])
        resolutions = [elem.text for elem in root.findall('.//ns:resolution', namespaces=ns)]
        found_res = var_resolution in resolutions
        for ts in root.findall('ns:TimeSeries', ns):
            name = ts.findtext(column_name, namespaces=ns)

            if var_name == "Actual Generation per Production Unit MAIN":
                name = ts.findtext('ns:MktPSRType/ns:PowerSystemResources/ns:mRID', namespaces=ns)
                registered_resource = ts.findtext('ns:registeredResource.mRID', namespaces=ns)
                ts_id = ts.findtext('ns:mRID', namespaces=ns)

                if not name: name = "no_name"
                if not registered_resource: registered_resource = "no_res"
                if not ts_id: ts_id = "no_ts_id"

                value_label = f"{var_name}__{registered_resource}__{ts_id}"

            if not name:
                logger.warning(f"No mRID found for TimeSeries in {var_name} {country_name} ({area_code}) – skipping this block.")
                continue
            name = name.strip()

            period = ts.find('ns:Period', ns)
            if period is None:
                logger.error(f"No  data for {var_name} {country_name} {request_params_str}")
                continue
            resolution = period.findtext('.//ns:resolution', namespaces=ns)

            if resolution != var_resolution:
                if found_res:
                    # W pliku XML są też TimeSeries z żądaną rozdzielczością — omiń inne
                    continue
                else:
                    # W XML nie ma TimeSeries z rozdzielczością z configa — przyjmij tę z XML
                    logger.warning(f"Resolution mismatch for {var_name} {country_name} ({area_code}). Using XML resolution '{resolution}' instead of config '{var_resolution}'")
                    var_resolution = resolution  # nadpisz dla bieżącego bloku


            timeInterval = period.findall('ns:timeInterval', ns)
            start = period.findtext('ns:timeInterval/ns:start', namespaces=ns)
            end = period.findtext('ns:timeInterval/ns:end', namespaces=ns)

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
                        continue

            if var_name == "Generation Forecasts Day Ahead MAIN":
                ts_id = ts.findtext('ns:mRID', namespaces=ns)
                in_zone = ts.findtext('ns:inBiddingZone_Domain.mRID', namespaces=ns)
                out_zone = ts.findtext('ns:outBiddingZone_Domain.mRID', namespaces=ns)

                if in_zone:
                    zone_type = "in"
                elif out_zone:
                    zone_type = "out"
                else:
                    zone_type = "unknown"

                value_label = f"{var_name}__{zone_type}"
                ###value_label = f"{var_name}"

            else:
                if column_name == 'ns:mRID':
                    value_label = f"{var_name}__{area_code}"
                    ###value_label = f"{var_name}"
                else:
                    value_label = f"{name}__{area_code}"
                    ###value_label = f"{var_name}"


            partial_df = pd.DataFrame.from_dict(data, orient='index', columns=["quantity"])
            partial_df.loc[:, "Period_Start"] = start
            partial_df.loc[:, "Period_End"] = end
            partial_df.loc[:, "Resolution"] = resolution
            partial_df.loc[:, "variable"] = value_label
            results_df = pd.concat([results_df, partial_df.reset_index(drop=False, names = 'Position' )], axis=0)

        if results_df.empty:
            logger.warning(f"Parsed XML with TimeSeries structure but no data rows extracted for {var_name} {country_name} {area_code}.")
            return pd.DataFrame()

        results_df["area_code"] = area_code

        #return results_df
        return {**extracted_data, "success": True, "df": results_df}
    
    except Exception as e:
        logger.error(f"[parse_xml] Error: {str(e)}")
        return {**extracted_data, "success": False, "error": str(e), "df": pd.DataFrame()}
