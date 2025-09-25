# from pyspark.sql import functions as F

from transforms.api import configure, transform_df, Input, Output
from cms.utils import melt_array
import os
from cms import configs

input_folder = configs.transform + "02 - schema applied"
output_folder = configs.transform + "03 - prepared"

# Note that hh has HCPSCDXX columns but is dealt with separately because we use the REV_DTXX
# columns for the dates - see hh_rev_dt.py
inputs = [
    "ip",
    "hs",
    "hh"
]

pkey_col_lookup = {
    "ip": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "ADMSN_DT", "DSCHRGDT", "THRU_DT", "TYPE_ADM", "PMT_AMT"],
    "hs": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "FROM_DT", "THRU_DT", "PMT_AMT"],
    "hh": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "FROM_DT", "THRU_DT", "PMT_AMT"]
}


def transform_generator():

    transforms = []
    for input_dataset in inputs:
        output_dataset = f"{input_dataset}_hcpscd_long"
        input_pkeys = pkey_col_lookup[input_dataset]

        @configure(profile=['DRIVER_MEMORY_EXTRA_LARGE'])
        @transform_df(
            Output(os.path.join(output_folder, output_dataset)),
            source_df=Input(os.path.join(input_folder, input_dataset)),
        )
        def compute(source_df, pkey_cols=input_pkeys):

            procedure_columns = ["HCPSCD{}".format(str(x).zfill(2)) for x in range(1, 46)]
            rev_date_cols = ["REV_DT{}".format(str(x).zfill(2)) for x in range(1, 46)]

            df_procedures_long = melt_array(
                source_df,
                pkey_cols,
                procedure_columns,
                "HCPS_col",
                "HCPSCD",
                7
            )

            df_dates_long = melt_array(
                source_df,
                pkey_cols,
                rev_date_cols,
                "REV_DT_col",
                "REV_DT",
                7
            )

            join_on = pkey_cols + ["col_num"]
            df_procedures_long = df_procedures_long.join(
                df_dates_long, on=join_on
            )

            df_procedures_long = df_procedures_long.select(pkey_cols + ["HCPSCD", "REV_DT", "HCPS_col", "col_num"]).distinct()

            return df_procedures_long

        transforms.append(compute)

    return transforms


TRANSFORMS = transform_generator()
