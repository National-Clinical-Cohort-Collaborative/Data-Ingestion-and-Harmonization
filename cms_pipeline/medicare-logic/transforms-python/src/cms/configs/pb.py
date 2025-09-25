from cms.configs import full_column_list

# Step 1
name = "pb"
column_list = full_column_list.pb

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["bid", "claim_id", "blgnpi", "tax_num", "expnsdt1", "expnsdt2", "PLCSRVC"]

# Step 3
dates = {
    "type": "static",
    "start": "EXPNSDT1",
    "end": "EXPNSDT2"
}
melting = {
    "dx": {
        "code": "ICD10CM",
        "column_group": {
            "column": "DGNSCD",
            "count": 12,
        },
        "column_list": ["PDGNS_CD", "LINEDGNS"]
    },
    "hcpc": {
        "code": "HCPCS",
        "column_list": ["HCPCS_CD"]
    }
}
