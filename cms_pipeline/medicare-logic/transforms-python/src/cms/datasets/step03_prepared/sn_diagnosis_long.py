from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output
from cms.utils import melt_array
import os
from cms import configs

input_folder = configs.transform + "02 - schema applied"
output_folder = configs.transform + "03 - prepared"

# Note that hh has DGNSCD01 columns but is dealt with separately because we use the REV_DTXX
# columns for the dates - see hh_rev_dt.py
inputs = [
    "sn"
]

pkey_col_lookup = {
    "sn": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "FROM_DT", "THRU_DT", "PMT_AMT", "ORGNPINM"]
}

diagnosis_columns_lookup = {
    "sn": ["AD_DGNS", *("DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 26))]
}

rev_center_cols = ["RVCNTR{}".format(str(x).zfill(2)) for x in range(1, 46)]


def transform_generator():

    transforms = []

    for input_dataset in inputs:

        output_dataset = f"{input_dataset}_diagnosis_long"
        input_pkeys = pkey_col_lookup[input_dataset]
        diagnosis_columns = diagnosis_columns_lookup[input_dataset]

        @configure(profile=['DRIVER_MEMORY_EXTRA_LARGE'])
        @transform_df(
            Output(os.path.join(output_folder, output_dataset)),
            source_df=Input(os.path.join(input_folder, input_dataset)),
        )
        def compute(source_df, pkey_cols=input_pkeys, diagnosis_cols=diagnosis_columns):

            df_diagnoses_long = melt_array(source_df, pkey_cols, diagnosis_cols, "DX_col", "DX", 7)
            df_rev_center_long = melt_array(source_df, pkey_cols, rev_center_cols, "RVCNTR_col", "RVCNTR", 7)
     
            # Add PDGNS_CD as another DX row
            df_primary_dx_code = source_df.select(pkey_cols + ["PDGNS_CD"])
            df_primary_dx_code = df_primary_dx_code.withColumnRenamed("PDGNS_CD", "DX")
            df_primary_dx_code = df_primary_dx_code.withColumn("DX_col", F.lit("PDGNS_CD"))
            df_primary_dx_code = df_primary_dx_code.withColumn("col_num", F.lit("00"))

            # Union df_primary_dx_code and df_diagnosis_long
            df_diagnoses_long = df_diagnoses_long.unionByName(df_primary_dx_code)

            # Add DGNS_E as another DX row too.
            df_line_dx_code = source_df.select(pkey_cols + ["DGNS_E"])
            df_line_dx_code = df_line_dx_code.withColumnRenamed("DGNS_E", "DX")
            df_line_dx_code = df_line_dx_code.withColumn("DX_col", F.lit("DGNS_E"))
            df_line_dx_code = df_line_dx_code.withColumn("col_num", F.lit("00"))

            # Union df_line_dx_code and df_diagnosis_long
            df_diagnoses_long = df_diagnoses_long.unionByName(df_line_dx_code)

            # Add in the RVCNTR data
            join_on = pkey_cols + ["col_num"]
            result_df = df_diagnoses_long.join(
                df_rev_center_long, on=join_on, how='outer'
            )

            return result_df

        transforms.append(compute)

    return transforms


TRANSFORMS = transform_generator()
