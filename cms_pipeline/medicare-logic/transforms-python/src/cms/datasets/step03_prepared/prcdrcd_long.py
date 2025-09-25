# from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output
from cms.utils import melt_array
# import pyspark.sql.functions as F
import os
from cms import configs

input_folder = configs.transform + "02 - schema applied"
output_folder = configs.transform + "03 - prepared"

inputs = [
    "ip",
    "opl",
    "hs"
]

pkey_col_lookup = {
    "ip": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "ADMSN_DT", "DSCHRGDT", "THRU_DT", "TYPE_ADM"],
    "opl": ["BID", "CLAIM_ID", "PROVIDER", "LINEITEM", "REV_DT", "REV_CNTR", "FROM_DT", "THRU_DT"],
    "hs": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "FROM_DT", "THRU_DT"]
}


def transform_generator():

    transforms = []
    for input_dataset in inputs:
        output_dataset = f"{input_dataset}_prcdrcd_long"
        input_pkeys = pkey_col_lookup[input_dataset]

        @configure(profile=['DRIVER_MEMORY_EXTRA_LARGE'])
        @transform_df(
            Output(os.path.join(output_folder, output_dataset)),
            source_df=Input(os.path.join(input_folder, input_dataset)),
        )
        def compute(source_df, pkey_cols=input_pkeys):

            procedure_cols = ["PRCDRCD{}".format(str(x).zfill(2)) for x in range(1, 26)]
            procedure_date_cols = ["PRCDRDT{}".format(str(x).zfill(2)) for x in range(1, 26)]

            df_procedures_long = melt_array(
                source_df,
                pkey_cols,
                procedure_cols,
                "PRCDRCD_col",
                "PRCDRCD",
                8
            )

            df_dates_long = melt_array(
                source_df,
                pkey_cols,
                procedure_date_cols,
                "PRCDRDT_col",
                "PRCDRDT",
                8
            )

            join_on = pkey_cols + ["col_num"]
            df_procedures_long = df_procedures_long.join(
                df_dates_long, on=join_on
            ).distinct()

            return df_procedures_long

        transforms.append(compute)

    return transforms


TRANSFORMS = transform_generator()
