from cms.configs import full_column_list

# Step 1
name = "lt"
column_list = full_column_list.lt

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["PSEUDO_ID", "MSIS_ID",  "CLM_NUM_ORIG",  "BLG_PRVDR_NPI", "SRVC_PRVDR_NPI", "REV_CNTR_CD", "ADMSN_DT", "DSCHRG_DT", "SRVC_BGN_DT", "SRVC_END_DT", 
        "MDCR_REIMBRSMT_TYPE_CD", "MDCD_PD_AMT", "BILLED_AMT", "CLM_TYPE_CD", "CLM_NUM_ADJ", "YEAR", "LINE_NUM"]

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
            "count": 5,
        },
        "column_list": ["ADMTG_DGNS_CD"]

    }
}
