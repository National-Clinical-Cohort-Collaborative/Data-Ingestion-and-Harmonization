# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F
from cms.utils import melt_array


@transform_df(
    Output("/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/opl_procedures_long"),
    source_df=Input("/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/02 - schema applied/opl"),
)
def compute(source_df):

    diagnosis_columns = ["PRCDRD{}".format(str(x).zfill(2)) for x in range(1, 26)]
    pkey_cols = ["BID", "CLAIM_ID", "PROVIDER",  "REV_DT", "REV_CNTR", "FROM_DT", "THRU_DT"]

    df_procedures_long = melt_array(
        source_df,
        pkey_cols,
        diagnosis_columns,
        "PX_col",
        "PX",
        7
    )

    # Needed for joining on visit_occurrence
    df_procedures_long = df_procedures_long.withColumn(
        "visit_concept_id",
        F.when(F.col("REV_CNTR").isin('0450', '0451', '0452', '0456', '0459', '0981'), F.lit(9203)).otherwise(
            F.lit(9202)
        )
    )

    # Since we have a lot of duplicate procedure codes with different REV_CTR, we look for distinct date/procedure/visit types
    df_procedures_long = df_procedures_long.select(
        "BID", "CLAIM_ID", "PROVIDER", "REV_DT", "FROM_DT", "THRU_DT", "PX_col", "PX", "visit_concept_id"
    ).distinct()

    return df_procedures_long
