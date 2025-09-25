from cms.configs import full_column_list

# Step 1
name = "de_hh_and_spo"
column_list = full_column_list.de_hh_and_spo

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["PSEUDO_ID", "MSIS_ID", "SUBMTG_STATE_CD", "YEAR"]

# Step 3
dates = NotImplementedError
melting = {
}
