from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output
from cms.utils import melt_array


@configure(profile=['DRIVER_MEMORY_EXTRA_LARGE', 'EXECUTOR_MEMORY_LARGE', 'NUM_EXECUTORS_64'])
@transform_df(
    Output("/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/hh_rev_dt"),
    source_df=Input("ri.foundry.main.dataset.7cc16ca6-d525-4c54-bc71-58f3c49a35b4"),
)
def compute(source_df):

    # Identify visit dates in hh by looking at REV_DTXX columns
    # Line up HCPSCDXX columns containing procedure code associated with a given REV_DT

    pkey_cols = ["BID", "CLAIM_ID", "PROVIDER", "SGMT_NUM"]
    rev_date_cols = ["REV_DT{}".format(str(x).zfill(2)) for x in range(1, 46)]
    rev_center_cols = ["RVCNTR{}".format(str(x).zfill(2)) for x in range(1, 46)]
    procedure_cols = ["HCPSCD{}".format(str(x).zfill(2)) for x in range(1, 46)]
    diagnosis_cols = ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 26)]

    df_rev_dates_long = melt_array(source_df, pkey_cols, rev_date_cols, "RV_DT_col", "REV_DT", 7)
    df_rev_center_long = melt_array(source_df, pkey_cols, rev_center_cols, "RVCNTR_col", "RVCNTR", 7)
    df_procedures_long = melt_array(source_df, pkey_cols, procedure_cols, "HCPSCD_col", "HCPSCD", 7)
    df_diagnoses_long = melt_array(source_df, pkey_cols, diagnosis_cols, "DX_col", "DX", 7)

    join_on = pkey_cols + ["col_num"]
    df_rev_dates_long = df_rev_dates_long.join(
        df_rev_center_long, on=join_on, how='left'
    ).join(
        df_procedures_long, on=join_on, how='left'
    ).join(
        df_diagnoses_long, on=join_on, how="left"
    )

    df_rev_dates_long = df_rev_dates_long.withColumn("visit_concept_id", F.lit(581476))

    return df_rev_dates_long
