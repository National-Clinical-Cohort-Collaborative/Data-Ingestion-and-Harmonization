from cms.configs import full_column_list

# Step 1
name = "opl"
column_list = full_column_list.opl

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["bid", "claim_id", "provider", "rev_dt", "rev_cntr"]

# Step 3
dates = {
    "type": "static",
    "start": "REV_DT",
    "end": "REV_DT"
}
melting = {
    "dx": {
        "code": "ICD10CM",
        "column_group": {
            "column": "DGNSCD",
            "count": 25,
        },
        "column_list": ["PDGNS_CD", "DGNS_E"]
    }
}
