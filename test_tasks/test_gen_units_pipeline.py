# import json
# import pickle
# import logging
# from datetime import datetime as dt
# from pendulum import datetime
# from typing import Dict, Any
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# import sys
# import os
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# from dags.hello_world import (
#     _generate_run_parameters_logic,
#     extract_from_api,
#     store_raw_xml,
#     parse_generation_units_metadata,
#     merge_all_dataframes,
# )

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)

# # Konfiguracja testowa
# HISTORICAL_YEAR = 2024
# POSTGRES_CONN_ID = "postgres_azure_vm"
# RAW_XML_TABLE_NAME = "raw_entsoe_generation_units"

# # Kontekst Airflowowy symulowany ręcznie
# test_context = {
#     'logical_date': datetime(HISTORICAL_YEAR, 1, 1, tz="UTC"),
#     'data_interval_start': datetime(HISTORICAL_YEAR, 1, 1, tz="UTC"),
#     'data_interval_end': datetime(HISTORICAL_YEAR + 1, 1, 1, tz="UTC"),
#     'dag_run': type('MockDagRun', (), {'run_id': 'test_1234'})()
# }

# # 🔁 1. Generowanie parametrów
# params_list = _generate_run_parameters_logic(HISTORICAL_YEAR)

# print(f"Params_list: {params_list}")

# all_dfs = []

# for i, task_param in enumerate(params_list):
#     print(f"\n🟡 Test [{i+1}/{len(params_list)}] dla: {task_param['task_run_metadata']['country_name']} - {task_param['task_run_metadata']['var_name']}")

#     # 🧪 2. Pobieranie danych z API
#     try:
#         result = extract_from_api.function(task_param, **test_context)
#         #print(f"Test {i+1}: result = {result}")
#     except Exception as e:
#         logging.error(f"❌ Błąd w extract_from_api: {e}")
#         continue

#     # 💾 3. Zapis do raw_xml (opcjonalny)
#     try:
#         inserted_id = store_raw_xml.function(result, db_conn_id=POSTGRES_CONN_ID, table_name=RAW_XML_TABLE_NAME)
#         print(f"✅ Zapisano XML z ID:")
#     except Exception as e:
#         logging.warning(f"⚠️ Błąd zapisu: {e}")

#     # 🔍 4. Parsowanie XML → DataFrame
#     try:
#         df = parse_generation_units_metadata.function(result)
#         print(df.head())
#         all_dfs.append(df)
#     except Exception as e:
#         logging.error(f"❌ Błąd parsowania: {e}")

# # 🧩 5. Łączenie danych
# if all_dfs:
#     df_final = merge_all_dataframes.function(all_dfs)
#     print(f"\n✅ Połączono {len(all_dfs)} DataFrame’ów. Finalny kształt: {df_final.shape}")
#     # 💾 Zapis do pliku Excel
#     df_final.to_excel(f"test_{HISTORICAL_YEAR}.xlsx", index=False)
#     print("📁 Zapisano wynik do pliku: test.xlsx")

# else:
#     print("⚠️ Brak danych do połączenia.")