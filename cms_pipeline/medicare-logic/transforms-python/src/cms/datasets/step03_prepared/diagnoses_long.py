from transforms.api import transform_df, Input, Output
from cms.utils import melt_array
import os
from cms import configs

input_folder = configs.transform + "02 - schema applied"
output_folder = configs.transform + "03 - prepared"

# Note that hh has DGNSCD01 columns but is dealt with separately because we use the REV_DTXX
# columns for the dates - see hh_rev_dt.py
inputs = [
    # "ip",
    "dm",
    # "hs",
    # "pb",
    # "hh"
]

pkey_col_lookup = {
    "ip": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "ADMSN_DT", "DSCHRGDT", "THRU_DT", "PMT_AMT"],
    # "hs": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM", "FROM_DT", "THRU_DT", "PMT_AMT"],
    "dm": ["BID", "CLAIM_ID", "TAX_NUM", "SGMT_NUM", "EXPNSDT1", "EXPNSDT2", "PMT_AMT", "PLCSRVC", "TAX_NUM_ID"],
    # "pb": ["BID", "CLAIM_ID", "BLGNPI", "TAX_NUM", "SGMT_NUM", "LINEITEM", "EXPNSDT1", "EXPNSDT2", "PMT_AMT", "PLCSRVC"],
    # "hh": ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM",  "FROM_DT", "THRU_DT", "PMT_AMT"]
}

diagnosis_columns_lookup = {
    "ip": ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 26)],
    # "hs": ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 26)],
    "dm": ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 13)],
    # "pb": ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 13)],
    "hh": ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 26)]
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

            df_diagnoses_long = melt_array(
                source_df,
                pkey_cols,
                diagnosis_cols,
                "DX_col",
                "DX",
                7
            )

            return df_diagnoses_long

        transforms.append(compute)

    return transforms


TRANSFORMS = transform_generator()
