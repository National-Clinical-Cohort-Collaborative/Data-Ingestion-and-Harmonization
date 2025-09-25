from cms.configs import full_column_list

# Step 1
name = "ot"
column_list = full_column_list.ot

# Step 2
schema = {
    # "column": "type"
}

pkey_columns = column_list

# Step 3
dates = {
    "type": "static",
    "start": "SRVC_BGN_DT",
    "end": "SRVC_END_DT"
}
melting = {
    "dx": {
        "code": "ICD10CM",
        "column_group": {
            "column": "DGNS_CD",
            "count": 2,
        }
    },
    "proc": {
        "code": "proc",
        "column_list": ["LINE_PRCDR_CD"]
    }
}
