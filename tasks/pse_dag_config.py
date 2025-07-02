from datetime import datetime

POSTGRES_CONN_ID = "postgres_azure_vm"
HTTP_CONN_ID = "PSE_API"
HISTORICAL_START_DATE = datetime(2025, 6, 1)

ENTITY_CONFIG = {
    "pomb-rbn": {
        "columns": [
            "ofc", "dtime", "period", "pofmax", "pofmin", "plofmax", "plofmin",
            "dtime_utc", "period_utc", "reserve_type", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_pomb_rbn",
        "production_table": "pomb_rbn"
    },
    "kmb-kro-rozl": {
        "columns": [
            "kmb", "kro", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_kmb_kro_rozl",
        "production_table": "kmb_kro_rozl"
    },
    "poeb-rbn": {
        "columns": [
            "ofp", "ofcd", "ofcg", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_poeb_rbn",
        "production_table": "poeb_rbn"
    },
    "poeb-rbb": {
        "columns": [
            "ofp", "ofcd", "ofcg", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_poeb_rbb",
        "production_table": "poeb_rbb"
    },
    "crb-rozl": {
        "columns": [
            "dtime", "period", "cen_cost", "dtime_utc", "ckoeb_cost",
            "period_utc", "ceb_pp_cost", "ceb_sr_cost",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_crb_rozl",
        "production_table": "crb_rozl"
    },
    "zmb": {
        "columns": [
            "dtime", "period", "zmb_rrd", "zmb_rrg", "zmb_fcrd", "zmb_fcrg", "zmb_frrd",
            "zmb_frrg", "dtime_utc", "zmb_afrrd", "zmb_afrrg", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_zmb",
        "production_table": "zmb"
    },
    "cmbp-tp": {
        "columns": [
            "onmb", "rr_d", "rr_g", "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g",
            "mfrrd_d", "mfrrd_g", "dtime_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_cmbp_tp",
        "production_table": "cmbp_tp"
    },
    "mbu-tu": {
        "columns": [
            "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g", "period", "mfrrd_d", "mfrrd_g",
            "dtime_utc", "period_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_mbu_tu",
        "production_table": "mbu_tu"
    },
    "mbp-tp": {
        "columns": [
            "rr_d", "rr_g", "dtime", "fcr_d", "fcr_g", "onmbp", "afrr_d", "afrr_g", "mfrrd_d",
            "mfrrd_g", "dtime_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_mbp_tp",
        "production_table": "mbp_tp"
    },
    "rce-pln": {
        "columns": [
            "dtime", "period", "rce_pln", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_rce_pln",
        "production_table": "rce_pln"
    },
    "csdac-pln": {
        "columns": [
            "dtime", "period", "csdac_pln", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_csdac_pln",
        "production_table": "csdac_pln"
    },
    "eb-rozl": {
        "columns": [
            "dtime", "period", "eb_d_pp", "eb_w_pp", "dtime_utc", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_eb_rozl",
        "production_table": "eb_rozl"
    },
    "cmbu-tu": {
        "columns": [
            "dtime", "fcr_d", "fcr_g", "afrr_d", "afrr_g", "period", "mfrrd_d", "mfrrd_g",
            "dtime_utc", "period_utc", "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_cmbu_tu",
        "production_table": "cmbu_tu"
    },
    "popmb-rmb": {
        "columns": [
            "com", "pom", "comrr", "dtime", "onmbp", "dtime_utc", "reserve_type",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_popmb_rmb",
        "production_table": "popmb_rmb"
    }
}

ENTITY_CONFIG_2 = {
    "poeb-rbb": {
        "columns": [
            "ofp", "ofcd", "ofcg", "dtime", "period",
            "dtime_utc", "period_utc", "business_date",
            "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_poeb_rbb",
        "production_table": "poeb_rbb"
    },
    "zmb": {
        "columns": [
            "dtime", "period", "zmb_rrd", "zmb_rrg", "zmb_fcrd", "zmb_fcrg", "zmb_frrd",
            "zmb_frrg", "dtime_utc", "zmb_afrrd", "zmb_afrrg", "period_utc",
            "business_date", "publication_ts", "publication_ts_utc"
        ],
        "staging_table": "stg_zmb",
        "production_table": "zmb"
    }
}