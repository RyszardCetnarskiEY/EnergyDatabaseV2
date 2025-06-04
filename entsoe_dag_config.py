ENTSOE_VARIABLES = {
    "Energy Prices fixing I": {
        "table": "energy_prices_day_ahead",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:mRID', "resolution" : 'PT60M'}, #ns will be inferred when parsing xml
        "params" : {"documentType": "A44", "contract_MarketAgreement.type": "A01"}
    },
    
    # "Generation Forecasts - Day ahead": {
    #     "table": "generation_forecasts_day_ahead",
    #     "AreaType" : "country_domain",
    #     'xml_parsing_info' : {"column_name" : 'ns:mRID'},
    #     "params" : {"documentType": "A71", "processType": "A01"}
    # },
    # "Actual Generation per Production Type": {
    #     "table": "actual_generation_per_production_type",
    #     "AreaType" : "country_domain",
    #     'xml_parsing_info' : {"column_name" : 'ns:MktPSRType/ns:psrType'},

    #     "params" : {"documentType": "A75", "processType": "A16"}
    # },

    "Generation Forecasts for Wind and Solar": {
        "table": "generation_forecasts_wind_solar",
        "AreaType" : "country_domain",
        'xml_parsing_info' : {"column_name" : 'ns:MktPSRType/ns:psrType', "resolution" : 'PT15M'},
        "params" : {"documentType": "A69", "processType": "A01"}
    },

        "Actual Generation per Generation Unit": {
        "table": "actual_generation_per_production_type",
        "AreaType" : "bidding_zones",
        'xml_parsing_info' : {"column_name" : "ns:MktPSRType/ns:PowerSystemResources/ns:name",  "resolution" :'PT60M'},
        "params" : {"documentType": "A73", "processType": "A16"}
    },

}
