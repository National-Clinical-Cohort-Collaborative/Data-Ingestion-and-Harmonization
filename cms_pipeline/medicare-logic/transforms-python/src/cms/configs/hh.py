from cms.configs import full_column_list

# Step 1
name = "hh"
column_list = full_column_list.hh

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["bid", "claim_id", "provider", "sgmt_num"]

# Step 3
dates = {
    "type": "dynamic",
    "start": "REV_DT",
    "end": None
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
