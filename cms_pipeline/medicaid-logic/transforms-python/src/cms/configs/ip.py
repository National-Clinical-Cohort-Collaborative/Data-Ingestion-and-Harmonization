from cms.configs import full_column_list

# Step 1
name = "ip"
column_list = full_column_list.ip

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["PSEUDO_ID", "MSIS_ID",  "CLM_NUM_ORIG", "BLG_PRVDR_NPI", "SRVC_PRVDR_NPI", "SRVC_PRVDR_ID", "REV_CNTR_CD", "BLG_PRVDR_SPCLTY_CD",
     "MDCR_REIMBRSMT_TYPE_CD", "ADMSN_DT", "DSCHRG_DT", "ADMSN_HR", "BILLED_AMT", "CLM_TYPE_CD", "YEAR", "LINE_NUM", "MDCD_PD_AMT", "CLAIMNO"]

# Step 3
dates = {
    "type": "static",
    "start": "ADMSN_DT",
    "end": "DSCHRG_DT"
}

melting = {
    "dx": {
        "code": "ICD10CM",
        "column_group": {
            "column": "DGNS_CD",
            "count": 12,
        },
        "column_list": ["ADMTG_DGNS_CD"]

    },
    "proc": {
        "code": "proc",
        "column_group": {
            "column": "PRCDR_CD",
            "count": 6,
        }
    }
}
