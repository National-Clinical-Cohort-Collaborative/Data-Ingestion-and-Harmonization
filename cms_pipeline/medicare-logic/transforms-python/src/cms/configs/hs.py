from cms.configs import full_column_list

# Step 1
name = "hs"
column_list = full_column_list.hs

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["bid", "claim_id", "provider", "sgmt_num", "from_dt", "thru_dt"]

# Step 3
dates = {
    "type": "static",
    "start": "FROM_DT",
    "end": "THRU_DT"
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
    "hcpc": {
        "code": "HCPCS",
        "column_group": {
            "column": "HCPSCD",
            "count": 45,
        }
    }
}
