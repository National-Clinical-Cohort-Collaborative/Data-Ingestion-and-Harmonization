from pyspark.sql import types as T
from cms.configs import full_column_list

# Step 1
name = "dm"
column_list = full_column_list.dm

# Step 2
schema = {
    "BID": T.LongType()
}
pkey_columns = ["bid", "claim_id", "tax_num", "sgmt_num", "expnsdt1", "expnsdt2"]

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
            "count": 12
        },
        "column_list": ["PDGNS_CD", "LINEDGNS"]
    },
    "hcpc": {
        "code": "HCPCS",
        "column_list": ["HCPCS_CD"]
    }
}
