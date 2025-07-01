ENTSOE_VARIABLES = {
    "Energy Prices fixing I": {
        "table": "energy_prices_day_ahead_fixing_I_MAIN",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:mRID', "resolution" : 'PT60M'},
        "params" : {"documentType": "A44", "contract_MarketAgreement.type": "A01"}
    },

    "Actual Total Load": {
        "table": "actual_total_load_MAIN",
        "AreaType": "country_domain",
        'xml_parsing_info': {"column_name": 'ns:mRID', "resolution": 'PT15M'},
        "params": {"documentType": "A65", "processType": "A16"}
    },

    "Day-ahead Total Load Forecast": {
        "table": "total_load_forecast_day_ahead_MAIN",
        "AreaType": "country_domain",
        'xml_parsing_info': {"column_name": 'ns:mRID', "resolution": 'PT15M'},
        "params": {"documentType": "A65", "processType": "A01"}
    },

    "Generation Forecasts for Wind and Solar": {
        "table": "generation_forecasts_wind_solar_MAIN",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:MktPSRType/ns:psrType', "resolution" : 'PT15M'},
        "params" : {"documentType": "A69", "processType": "A01"}
    },

    "Actual Generation per Generation Unit": {
        "table": "actual_generation_per_production_unit_MAIN",
        "AreaType" : "bidding_zones",
        'xml_parsing_info' : {"column_name" : "ns:MktPSRType/ns:PowerSystemResources/ns:name",  "resolution" :'PT60M'},
        "params" : {"documentType": "A73", "processType": "A16"}
    },

    "Generation Forecasts - Day ahead": {
        "table": "generation_forecasts_day_ahead_MAIN",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:mRID', "resolution" :'PT60M'},
        "params" : {"documentType": "A71", "processType": "A01"}
    }
}

POSTGRES_CONN_ID = "postgres_azure_vm"
RAW_XML_TABLE_NAME = "entsoe_raw_xml_landing"

# https://www.entsoe.eu/data/energy-identification-codes-eic/eic-area-codes-map/

COUNTRY_MAPPING = {
    "PL": {"name": "Poland", "country_domain": ["10YPL-AREA-----S"], "bidding_zones" : ["10YPL-AREA-----S"]},
    "DE": {"name": "Germany", "country_domain": ["10Y1001A1001A82H"], "bidding_zones": ["10YDE-VE-------2", "10YDE-EON------1", "10YDE-ENBW-----N", "10YDE-RWENET---I"]},
    "CZ": {"name": "Czech Republic", "country_domain": ["10YCZ-CEPS-----N"], "bidding_zones" : ["10YCZ-CEPS-----N"]},
}

# API_BASE_URL = "https://web-api.tp.entsoe.eu/api"

# "LT": {"name": "Lithuania", "domain": "10YLT-1001A0008Q"},
# "CZ": {"name": "Czech Republic", "country_domain": ["10YCZ-CEPS-----N"], "bidding_zones" : ["10YCZ-CEPS-----N"]},
# "SK": {"name": "Slovakia", "domain": "10YSK-SEPS-----K"},
# "SE": {"name": "Sweden", "domain": "10YSE-1--------K"},
# "FR": {"name": "France", "domain": "10YFR-RTE------C"},