from cms.configs import full_column_list

# Step 1
name = "de_wvr"
column_list = full_column_list.de_mc

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["PSEUDO_ID", "MSIS_ID", "SUBMTG_STATE_CD", "YEAR"]

# Step 3
dates = {
}
melting = {
}
