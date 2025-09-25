from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms.utils import melt_array
import os
from cms import configs

input_folder = configs.transform + "02 - schema applied"
output_folder = configs.transform + "03 - prepared"

# Note that hh has DGNSCD01 columns but is dealt with separately because we use the REV_DTXX
# columns for the dates - see hh_rev_dt.py

inputs = [
    "pb"
]

pkey_col_lookup = {
    "pb": ["BID", "CLAIM_ID", "BLGNPI", "TAX_NUM", "SGMT_NUM", "LINEITEM", "EXPNSDT1", "EXPNSDT2", "PMT_AMT", "PLCSRVC", "TAX_NUM_ID"],
}

diagnosis_columns_lookup = {
    "pb": ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 13)],
}


def transform_generator():
    transforms = []
    for input_dataset in inputs:
        output_dataset = f"{input_dataset}_diagnosis_long"
        input_pkeys = pkey_col_lookup[input_dataset]
        diagnosis_columns = diagnosis_columns_lookup[input_dataset]

        @transform_df(
            Output(os.path.join(output_folder, output_dataset)),
            source_df=Input(os.path.join(input_folder, input_dataset)),
        )
        def compute(source_df, pkey_cols=input_pkeys, diagnosis_cols=diagnosis_columns):
            df_diagnosis_long = melt_array(
                source_df,
                pkey_cols,
                diagnosis_cols,
                "DX_col",
                "DX",
                7
            )

            df_primary_dx_code = source_df.select(pkey_cols + ["PDGNS_CD"])
            df_primary_dx_code = df_primary_dx_code.withColumnRenamed("PDGNS_CD", "DX")
            df_primary_dx_code = df_primary_dx_code.withColumn("DX_col", F.lit("PDGNS_CD"))
            df_primary_dx_code = df_primary_dx_code.withColumn("col_num", F.lit("00"))

            # Union df_primary_dx_code and df_diagnosis_long
            df_diagnosis_long = df_diagnosis_long.unionByName(df_primary_dx_code)
            df_line_dx_code = source_df.select(pkey_cols + ["LINEDGNS"])
            df_line_dx_code = df_line_dx_code.withColumnRenamed("LINEDGNS", "DX")
            df_line_dx_code = df_line_dx_code.withColumn("DX_col", F.lit("LINEDGNS"))
            df_line_dx_code = df_line_dx_code.withColumn("col_num", F.lit("00"))

            # Union df_primary_dx_code and df_diagnosis_long
            df_diagnosis_long = df_diagnosis_long.unionByName(df_line_dx_code)

            return df_diagnosis_long

        transforms.append(compute)

    return transforms


TRANSFORMS = transform_generator()
