from cms.configs import full_column_list

# Step 1
name = "de_dates"
column_list = full_column_list.de_dates

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["PSEUDO_ID", "MSIS_ID", "SUBMTG_STATE_CD", "YEAR"]

# Step 3
dates = {
    "type": "static",
    "start": "ENRLMT_START_DT",
    "end": "ENRLMT_END_DT"
}
melting = {
}
