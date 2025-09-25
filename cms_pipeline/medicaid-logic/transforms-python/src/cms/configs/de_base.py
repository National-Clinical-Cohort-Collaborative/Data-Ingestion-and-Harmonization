from cms.configs import full_column_list

# Step 1
name = "de_base"
column_list = full_column_list.de_base

# Step 2
schema = {
    # "column": "type"
}
pkey_columns = ["PSEUDO_ID", "MSIS_ID", "MSIS_CASE_NUM", "YEAR"]

# Step 3
dates = {
    "type": "static",
    "start": "BIRTH_DT",
    "end": "DEATH_DT"
}
melting = {
}
