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
    if not extracted_data.get("success", False):
        logger.warning("[store_raw_xml] upstream failed: %s", extracted_data.get("error"))
        return {**extracted_data, "stored": False, "df": pd.DataFrame()}

    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    sql = f"""
    INSERT INTO airflow_data."{table_name}"
        (var_name, country_name, area_code, status_code, period_start, period_end, request_time, xml_data, request_parameters, content_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
    RETURNING id;"""

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
    #return {**extracted_data, "stored": True, "raw_id": inserted_id}
    result = {**extracted_data, "stored": True, "raw_id": inserted_id, "df": pd.DataFrame()}
    logger.info("[store_raw_xml] inserted raw_id=%s into %s", inserted_id, table_name)
    return result

def handle_actual_generation_per_production_unit(ts, var_name, ns):
    # Obsługuje przypadek "Actual Generation per Production Unit MAIN"
    name = ts.findtext('ns:MktPSRType/ns:PowerSystemResources/ns:mRID', namespaces=ns)
    registered_resource = ts.findtext('ns:registeredResource.mRID', namespaces=ns)
    ts_id = ts.findtext('ns:mRID', namespaces=ns)

    if not name: name = "no_name"
    if not registered_resource: registered_resource = "no_res"
    if not ts_id: ts_id = "no_ts_id"

    #value_label = f"{var_name}__{registered_resource}__{ts_id}"
    value_label = f"{var_name}__{registered_resource}__{ts_id}__{name}"
    return value_label, name, registered_resource, ts_id

def handle_generation_forecasts_day_ahead(ts, var_name, ns, column_name, area_code):
    # Obsługuje przypadek "Generation Forecasts Day Ahead MAIN"
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
    return value_label, ts_id, in_zone, out_zone

def _minutes_from_iso_duration(res: str) -> int:
    # wspieramy najczęstsze: PT15M, PT30M, PT60M
    if not res or not res.startswith("PT") or not res.endswith("M"):
        raise ValueError(f"Unsupported resolution: {res}")
    return int(res[2:-1])

@task(task_id='parse_xml')
def parse_xml(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if not extracted_data.get("success", False):
            return {**extracted_data, "success": False, "error": "Upstream failure in extract_from_api", "df": pd.DataFrame()}

        xml_data     = extracted_data['xml_content']
        country_name = extracted_data['country_name']
        area_code    = extracted_data['area_code']
        var_name     = extracted_data['var_name']
        column_name  = extracted_data['task_run_metadata']['config_dict']["xml_parsing_info"]['column_name']
        var_res_cfg  = extracted_data['task_run_metadata']['config_dict']["xml_parsing_info"]['resolution']
        req_params   = extracted_data['request_params']

        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML format: {e}")

        ns = {'ns': root.tag[root.tag.find("{")+1:root.tag.find("}")]} if "{" in root.tag else {}

        # Czy w ogólnym XML gdziekolwiek występuje oczekiwana rozdzielczość?
        resolutions = [elem.text for elem in root.findall('.//ns:resolution', namespaces=ns)]
        found_cfg_res_anywhere = (var_res_cfg in resolutions)

        parts: List[pd.DataFrame] = []

        for ts in root.findall('ns:TimeSeries', ns):
            # --- nazwa z XML (może być None) ---
            name = ts.findtext(column_name, namespaces=ns)
            if name:
                name = name.strip()
            else:
                name = ""

            value_label = f"{var_name}__{area_code}"

            # Specjalne przypadki – mogą nadpisać label
            if var_name == "Actual Generation per Production Unit MAIN":
                try:
                    value_label, name, registered_resource, ts_id = handle_actual_generation_per_production_unit(ts, var_name, ns)
                except Exception:
                    # w razie czego zachowaj fallback ustawiony wyżej
                    pass
            elif var_name == "Generation Forecasts Day Ahead MAIN":
                try:
                    value_label, ts_id, in_zone, out_zone = handle_generation_forecasts_day_ahead(ts, var_name, ns, column_name, area_code)
                except Exception:
                    pass
            else:
                # Dla zwykłych przypadków – jeśli masz sensowną nazwę i kolumna nie jest mRID
                if column_name != 'ns:mRID' and name:
                    value_label = f"{name}__{area_code}"

            period = ts.find('ns:Period', ns)
            if period is None:
                logger.error(f"No data for {var_name} {country_name} {req_params}")
                continue

            # Uzgodnienie rozdzielczości – trzymaj się configu, jeśli występuje w XML
            res_xml = period.findtext('.//ns:resolution', namespaces=ns) or var_res_cfg
            if res_xml != var_res_cfg:
                if found_cfg_res_anywhere:
                    # w XML w ogóle istnieje oczekiwana rozdzielczość – pomiń inne
                    continue
                else:
                    # nie ma oczekiwanej – jedziemy na tej z XML
                    logger.warning(
                        f"Resolution mismatch for {var_name} {country_name} ({area_code}). "
                        f"Using XML resolution '{res_xml}' instead of config '{var_res_cfg}'"
                    )
                    var_res_cfg = res_xml

            start = period.findtext('ns:timeInterval/ns:start', namespaces=ns)
            end   = period.findtext('ns:timeInterval/ns:end', namespaces=ns)

            # Zbierz punkty: pozycje 1-based z XML, wartości liczbowe lub NaN
            pos1_list: List[int] = []
            data0: Dict[int, float] = {}

            for point in period.findall('ns:Point', ns):
                pos_txt = point.findtext('ns:position', namespaces=ns)
                qty_txt = point.findtext('ns:quantity', namespaces=ns) or point.findtext('ns:price.amount', namespaces=ns)
                if pos_txt is None:
                    continue
                try:
                    pos1 = int(pos_txt)
                    pos0 = pos1 - 1     
                except Exception:
                    continue
                pos1_list.append(pos1)
                try:
                    qty = float(qty_txt) if qty_txt is not None else float("nan")
                except Exception:
                    qty = float("nan")
                data0[pos0] = qty

            if not pos1_list:
                continue

            expected_points = max(pos1_list)

            partial_df = pd.DataFrame.from_dict(data0, orient='index', columns=["quantity"])

            full_index = pd.RangeIndex(expected_points)
            partial_df = partial_df.reindex(full_index)

            partial_df["Position"]     = partial_df.index.astype("int64") + 1
            partial_df["Period_Start"] = start
            partial_df["Period_End"]   = end
            partial_df["Resolution"]   = var_res_cfg
            partial_df["variable"]     = value_label
            partial_df["area_code"]    = area_code

            # kolejność kolumn jak w testach
            partial_df = partial_df[["Position","Period_Start","Period_End","Resolution","quantity","variable","area_code"]]

            parts.append(partial_df)

        if not parts:
            return {**extracted_data, "success": False, "error": "No data extracted", "df": pd.DataFrame()}

        # ważne: bez ignore_index – powtarzający się indeks 0..N-1 per Period
        results_df = pd.concat(parts, ignore_index=False)
        results_df["Position"] = results_df["Position"].astype("int64")

        if results_df["quantity"].isna().any():
            grp = ["Period_Start", "Period_End", "Resolution", "variable", "area_code"]

            # 1) Usuń NaN-y
            results_df = results_df[results_df["quantity"].notna()].copy()

            # 2) Stabilna kolejność: po grupie i oryginalnym Position
            results_df = results_df.sort_values(grp + ["Position"])

            # 3) NIE zmieniamy values w kolumnie Position!
            results_df["Position"] = results_df["Position"].astype("int64")

            # 4) Odbuduj indeks 0..K-1 per okres, ale bez tykania Position
            new_index = results_df.groupby(grp).cumcount()
            results_df = results_df.set_index(new_index)
            results_df.index.name = None

            # 5) Kolejność kolumn jak w testach
            results_df = results_df[["Position","Period_Start","Period_End","Resolution","quantity","variable","area_code"]]

        return {**extracted_data, "success": True, "df": results_df}

    except Exception as e:
        logger.exception("[parse_xml] error")
        return {**extracted_data, "success": False, "error": str(e), "df": pd.DataFrame()}