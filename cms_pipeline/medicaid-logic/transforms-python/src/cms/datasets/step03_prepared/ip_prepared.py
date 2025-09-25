from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


@transform_df(
    Output(configs.transform + "03 - prepared/" + "ip_prepared"),
    ip=Input("ri.foundry.main.dataset.2b867813-7600-4140-93ee-0c757dba6aea"),
)
def compute(ip):
    ip = ip.withColumn("BILL_TYPE_CD_digit1", F.substring(ip.BILL_TYPE_CD, 1, 1))
    ip = ip.withColumn("BILL_TYPE_CD_digit2", F.substring(ip.BILL_TYPE_CD, 2, 1))
    ip = ip.withColumn("BILL_TYPE_CD_digit3", F.substring(ip.BILL_TYPE_CD, 3, 1))
    ip = ip.withColumn("BILL_TYPE_CD_digit4", F.substring(ip.BILL_TYPE_CD, 4, 1))

    ip = ip.withColumn("ADMSN_HR", F.when(F.col("ADMSN_HR").isNull(), "00").otherwise(F.col("ADMSN_HR")))
    ip = ip.withColumn("ADMSN_HR", F.when(F.col("ADMSN_HR") == '', "00").otherwise(F.col("ADMSN_HR")))
    ip = ip.withColumn("admsn_dt_concat", F.concat(F.col("ADMSN_DT"), F.col("ADMSN_HR")))
    ip = ip.withColumn("admission_datetime", F.to_timestamp(F.col("admsn_dt_concat"), "ddMMMyyyyHH"))

    return ip
