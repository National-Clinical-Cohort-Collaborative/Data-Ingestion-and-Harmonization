# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F
from cms.utils import melt_array


@transform_df(
    Output("/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/ip_visits"),
    source_df=Input("ri.foundry.main.dataset.89592151-7967-4147-8bc3-9f2ad12b62a6"),
)
def compute(source_df):
    # Identify claims in ip that correspond to an ER visit by looking
    # at RVCNTRXX columns
    rev_center_cols = ["RVCNTR{}".format(str(x).zfill(2)) for x in range(1, 46)]

    pkey_cols = ["BID", "CLAIM_ID", "SGMT_NUM", "PROVIDER", "ADMSN_DT", "DSCHRGDT", "THRU_DT", "TYPE_ADM", "PMT_AMT"]

    df_rev_codes_long = melt_array(
        source_df,
        pkey_cols,
        rev_center_cols,
        "RVCNTR_col",
        "REV_CTR",
        7
    )

    df_ER = df_rev_codes_long.withColumn(
        "is_er",
        F.col("REV_CTR").isin(["0450", "0451", "0452", "0456", "0459", "0981"])
        | F.col("TYPE_ADM").isin(["1", "5"])
    )

    result_df = df_ER.withColumn("visit_concept_id", F.when(F.col("is_er"), 262).otherwise(9201))

    return result_df
