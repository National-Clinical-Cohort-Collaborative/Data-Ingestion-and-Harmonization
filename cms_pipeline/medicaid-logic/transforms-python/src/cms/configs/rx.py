from cms.configs import full_column_list

# Step 1
name = "rx"
column_list = full_column_list.rx

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["PSEUDO_ID", "MSIS_ID",  "CLM_NUM_ORIG", "BLG_PRVDR_NPI", "PRSCRBNG_PRVDR_NPI", "DSPNSNG_PRVDR_NPI", "NDC", "NDC_QTY", "NEW_RX_REFILL_NUM", "DAYS_SUPPLY", 
"PRSCRBD_DT","MDCD_PD_DT", "BILLED_AMT", "MDCD_PD_AMT", "YEAR", "LINE_NUM", "LINE_NUM_ADJ", "CLAIMNO"]

# Step 3
dates = {
    "type": "dynamic",
    "start": "MDCD_PD_DT",
    "end": None
}
# NDC - NDC
melting = {
    "ndc": {
        "code": "NDC",
        "column_list": ["NDC"]
    }
}
