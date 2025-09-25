from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs
from pyspark.sql.types import IntegerType

inputs = {
    "ip_dx": "ip",
    "ip_proc": "ip",
    "lt_dx": "lt",
    "ot_dx": "ot",
    "ot_proc": "ot",
    "rx_ndc": "rx"
}


def make_transform(inp):
    dataset = inp
    domain = inputs[inp]

    for dom in configs.datasets:
        if dom.name == domain:
            pkey_cols = dom.pkey_columns
    if domain == "ot":
        pkey_cols = ["PSEUDO_ID", "BLG_PRVDR_NPI", "SRVC_PRVDR_NPI", "SRVC_BGN_DT", "SRVC_END_DT", "DGNS_POA_IND_1", "DGNS_POA_IND_2", "BLG_PRVDR_SPCLTY_CD", "POS_CD", "MDCR_REIMBRSMT_TYPE_CD","CLM_TYPE_CD",
         "MDCD_PD_AMT", "REV_CNTR_CD", "LINE_PRCDR_CD", "LINE_PRCDR_CD_DT", "LINE_PRCDR_CD_SYS", "LINE_PRCDR_MDFR_CD_1", "LINE_PRCDR_MDFR_CD_2",
        "LINE_PRCDR_MDFR_CD_3", "LINE_PRCDR_MDFR_CD_4"]

    if dataset == "ip_proc":
        pkey_cols.extend(["PRCDR_CD_DT_1", "PRCDR_CD_DT_2", "PRCDR_CD_DT_3", "PRCDR_CD_DT_4", "PRCDR_CD_DT_5", "PRCDR_CD_DT_6"])
        pkey_cols.extend(["PRCDR_CD_SYS_1", "PRCDR_CD_SYS_2", "PRCDR_CD_SYS_3", "PRCDR_CD_SYS_4", "PRCDR_CD_SYS_5", "PRCDR_CD_SYS_6"])

    @transform_df(
        Output(configs.transform + "03 - prepared/" + dataset + "_long_prepared"),
        step2_full=Input(configs.transform +  "02 - schema applied/" + domain),
        step3_long=Input(configs.transform + "03 - melted/initial/" + dataset)
    )
    def compute(step2_full, step3_long):
        if "BILL_TYPE_CD" in step2_full.columns:
            step2_full = step2_full.withColumn("BILL_TYPE_CD_digit2", F.substring(step2_full.BILL_TYPE_CD, 2, 1)) 
            step2_full = step2_full.withColumn("BILL_TYPE_CD_digit3", F.substring(step2_full.BILL_TYPE_CD, 3, 1))
            pkey_cols.append("BILL_TYPE_CD")
            pkey_cols.append("BILL_TYPE_CD_digit2")
            pkey_cols.append("BILL_TYPE_CD_digit3")
        step2_full = step2_full.select(["pkey", *pkey_cols])
        ret = step2_full.join(step3_long, "pkey")

        if dataset == "ip_proc":
            ret = ret.withColumn("PRCDR_CD_DT",
                F.when(ret.index=="1", ret.PRCDR_CD_DT_1)\
                .when(ret.index=="2", ret.PRCDR_CD_DT_2)\
                .when(ret.index=="3", ret.PRCDR_CD_DT_3)\
                .when(ret.index=="4", ret.PRCDR_CD_DT_4)\
                .when(ret.index=="5", ret.PRCDR_CD_DT_5)\
                .when(ret.index=="6", ret.PRCDR_CD_DT_6)\
                .otherwise(F.lit(""))
            )

            ret = ret.withColumn("PRCDR_CD_SYS", F.when(ret.index=="1", ret.PRCDR_CD_SYS_1)\
                .when(ret.index=="2", ret.PRCDR_CD_SYS_2)\
                .when(ret.index=="3", ret.PRCDR_CD_SYS_3)\
                .when(ret.index=="4", ret.PRCDR_CD_SYS_4)\
                .when(ret.index=="5", ret.PRCDR_CD_SYS_5)\
                .when(ret.index=="6", ret.PRCDR_CD_SYS_6)\
                .otherwise(F.lit(""))
            )

            ret = ret.withColumn("PRCDR_CD_SYS", ret.PRCDR_CD_SYS.cast(IntegerType()))

            ret = ret.withColumn("mapped_src_vocab_cd", F.when(ret.PRCDR_CD_SYS==1, F.lit("CPT4"))\
                .when(ret.PRCDR_CD_SYS==2, F.lit("ICD9CM"))\
                .when(ret.PRCDR_CD_SYS==6, F.lit("HCPCS"))\
                .when(ret.PRCDR_CD_SYS==7, F.lit("ICD10PCS"))\
                .when((ret.PRCDR_CD_SYS >= 10) & (ret.PRCDR_CD_SYS <= 87), F.lit("Other Systems"))\
                .otherwise(F.lit("ICD10PCS"))
            )

        return ret

    return compute


transforms = (make_transform(inp) for inp in inputs.keys())
