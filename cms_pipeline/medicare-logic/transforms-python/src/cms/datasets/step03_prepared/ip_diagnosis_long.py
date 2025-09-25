from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms.utils import melt_array
import os
from cms import configs

input_folder = configs.transform + "02 - schema applied"
output_folder = configs.transform + "03 - prepared"

# Note that hh has DGNSCD01 columns but is dealt with separately because we use the REV_DTXX
# columns for the dates - see hh_rev_dt.py
# separated from diagnoses_long to add the visit concept id
inputs = [
    "ip",
]

pkey_col_lookup = {
    "ip": ["BID", "CLAIM_ID", "PROVIDER", "ADMSN_DT", "DSCHRGDT", "THRU_DT", "TYPE_ADM", "SGMT_NUM", "PMT_AMT"]
}

diagnosis_columns_lookup = {
    "ip": ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 26)],
}

revcntr_columns_lookup = {
    "ip": ["RVCNTR{}".format(str(x).zfill(2)) for x in range(1, 46)],
}


def transform_generator():

    transforms = []

    for input_dataset in inputs:

        output_dataset = f"{input_dataset}_diagnosis_long"
        input_pkeys = pkey_col_lookup[input_dataset]
        diagnosis_columns = diagnosis_columns_lookup[input_dataset]
        revcntr_columns = revcntr_columns_lookup[input_dataset]

        @transform_df(
            Output(os.path.join(output_folder, output_dataset)),
            source_df=Input(os.path.join(input_folder, input_dataset)),
        )
        def compute(source_df, pkey_cols=input_pkeys, diagnosis_cols=diagnosis_columns):

            df_diagnoses_long = melt_array(
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
            df_diagnoses_long = df_diagnoses_long.unionByName(df_primary_dx_code)
            df_line_dx_code = source_df.select(pkey_cols + ["DGNS_E"])
            df_line_dx_code = df_line_dx_code.withColumnRenamed("DGNS_E", "DX")
            df_line_dx_code = df_line_dx_code.withColumn("DX_col", F.lit("DGNS_E"))
            df_line_dx_code = df_line_dx_code.withColumn("col_num", F.lit("00"))

            # Union df_primary_dx_code and df_diagnosis_long
            df_diagnoses_long = df_diagnoses_long.unionByName(df_line_dx_code)

            # add rev center code cols RVCNTR01 to RVCNTR45
            RVCNTR_cols = ["RVCNTR{}".format(str(x).zfill(2)) for x in range(1, 46)]
            df_rev_cntr_long = melt_array(source_df, pkey_cols, RVCNTR_cols, "RVCNTRCD_col", "RVCNTR_CD", 7)
            join_on = pkey_cols + ["col_num"]
            df_diagnoses_long = df_diagnoses_long.join(df_rev_cntr_long, on=join_on, how='left')

            return df_diagnoses_long

        transforms.append(compute)

    return transforms


TRANSFORMS = transform_generator()
