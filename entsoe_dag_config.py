ENTSOE_VARIABLES = {

    "Energy Prices": {
        "table": "energy_prices_day_ahead",
        "AreaType" : "bidding_zones",
        "params" : {"documentType": "A44", "contract_MarketAgreement.type": "A01"}
    },
    "Generation Forecasts - Day ahead": {
        "table": "generation_forecasts_day_ahead",
        "AreaType" : "country_domain",

        "params" : {"documentType": "A71", "processType": "A01"}
    },
    "Actual Generation per Production Type": {
        "table": "actual_generation_per_production_type",
        "AreaType" : "country_domain",

        "params" : {"documentType": "A75", "processType": "A16"}
    },

    "Generation Forecasts for Wind and Solar": {
        "table": "generation_forecasts_wind_solar",
        "AreaType" : "country_domain",

        "params" : {"documentType": "A69", "processType": "A01"}
    },

}

    ### Tu trzeba rozbić Niemcy na poszczególnych operatorów in_Domain - [M] EIC code of a Control Area

    # "Actual Generation per Generation Unit": {
    #     "table": "actual_generation_per_production_type"
    #     "params" : {"document_type": "A73", "processType": "A16"}

    #     #"parser": "parse_generation",
    # },


# 50hz:
# 10YDE-VE-------2

# Tennet:
# 10YDE-EON------1	

# Amprion:
# 10Y1001C--00002H	

# Transnet: 
# 10YDE-ENBW-----N	