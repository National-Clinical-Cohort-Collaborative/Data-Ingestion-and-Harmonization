from cms.configs import full_column_list

# Step 1
name = "ip"
column_list = full_column_list.ip

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["bid", "claim_id", "provider", "sgmt_num", "admsn_dt", "dschrgdt", "TYPE_ADM"]

# Step 3
dates = {
    "type": "static",
    "start": "ADMSN_DT",
    "end": "DSCHRGDT"
}
melting = {
    "dx": {
        "code": "ICD10CM",
        "column_group": {
            "column": "DGNSCD",
            "count": 25,
        },
        "column_list": ["PDGNS_CD", "DGNS_E"]
    },
    "px": {
        "code": "ICD10PCS",
        "column_group": {
            "column": "PRCDRCD",
            "count": 25,
        }
    },
    "hcpc": {
        "code": "HCPCS",
        "column_group": {
            "column": "HCPSCD",
            "count": 45,
        }
    }
}
