from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F
from cms.utils import melt_array


@transform_df(
    Output("/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/opl_diagnoses_long"),
    source_df=Input("ri.foundry.main.dataset.a349b96d-d472-4a87-8eab-a69804e7ec0a"),
)
def compute(source_df):

    EDGN_cols = ["EDGNCD{}".format(str(x).zfill(2)) for x in range(1, 12)]
    diagnosis_columns = ["DGNSCD{}".format(str(x).zfill(2)) for x in range(1, 26)]
    pkey_cols = ["BID", "CLAIM_ID", "PROVIDER", "REV_DT", "REV_CNTR", "FROM_DT", "THRU_DT", "PMT_AMT"]

    df_diagnoses_long = melt_array(
        source_df,
        pkey_cols,
        diagnosis_columns,
        "DX_col",
        "DX",
        7
    )

    # Needed for joining on visit_occurrence
    df_diagnoses_long = df_diagnoses_long.withColumn(
        "visit_concept_id",
        F.when(F.col("REV_CNTR").isin('0450', '0451', '0452', '0456', '0459', '0981'), F.lit(9203)).otherwise(
            F.lit(9202)
        )
    )

    # Since we have a lot of duplicate diagnosis codes with different REV_CTR, we look for distinct date/diagnosis/visit types
    df_diagnoses_long = df_diagnoses_long.select(
        "BID", "CLAIM_ID", "PROVIDER", "REV_DT", "REV_CNTR", "FROM_DT", "THRU_DT", "PMT_AMT", "DX_col", "DX", "col_num", "visit_concept_id"
    ).distinct()

    df_primary_dx_code = source_df.select(pkey_cols + ["PDGNS_CD"])
    df_primary_dx_code = df_primary_dx_code.withColumnRenamed("PDGNS_CD", "DX")
    df_primary_dx_code = df_primary_dx_code.withColumn("DX_col", F.lit("PDGNS_CD"))
    df_primary_dx_code = df_primary_dx_code.withColumn("col_num", F.lit("00"))
    df_primary_dx_code = df_primary_dx_code.withColumn("visit_concept_id", F.lit(None))

    # Union df_primary_dx_code and create df_diagnosis_long
    df_diagnoses_long = df_diagnoses_long.unionByName(df_primary_dx_code)

    df_line_dx_code = source_df.select(pkey_cols + ["DGNS_E"])
    df_line_dx_code = df_line_dx_code.withColumnRenamed("DGNS_E", "DX")
    df_line_dx_code = df_line_dx_code.withColumn("DX_col", F.lit("DGNS_E"))
    df_line_dx_code = df_line_dx_code.withColumn("col_num", F.lit("00"))
    df_line_dx_code = df_line_dx_code.withColumn("visit_concept_id", F.lit(None))

    # Union df_primary_dx_code and df_diagnosis_long
    df_diagnoses_long = df_diagnoses_long.unionByName(df_line_dx_code)

    # Add EDGNCD01 to EDGNCD12 probably with code that looks something like the following
    df_edgn_long = melt_array(source_df, pkey_cols, EDGN_cols, "EDGNCD_col", "EDGNCD", 7)

    join_on = pkey_cols + ["col_num"]
    df_diagnoses_long = df_diagnoses_long.join(
        df_edgn_long, on=join_on, how='left'
    )

    return df_diagnoses_long
