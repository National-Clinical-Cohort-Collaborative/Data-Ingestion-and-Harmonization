from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


@transform_df(
    Output(configs.transform + "03 - prepared/" + "lt_prepared"),
    lt=Input("ri.foundry.main.dataset.b11642ff-0086-4c35-aaa1-a908841b55f0")
)
def compute(lt):
    lt = lt.withColumn("BILL_TYPE_CD_digit1", F.substring(lt.BILL_TYPE_CD, 1, 1))
    lt = lt.withColumn("BILL_TYPE_CD_digit2", F.substring(lt.BILL_TYPE_CD, 2, 1))
    lt = lt.withColumn("BILL_TYPE_CD_digit3", F.substring(lt.BILL_TYPE_CD, 3, 1))
    lt = lt.withColumn("BILL_TYPE_CD_digit4", F.substring(lt.BILL_TYPE_CD, 4, 1))

    lt = lt.withColumn("ADMSN_HR", F.when(F.col("ADMSN_HR").isNull(), "00").otherwise(F.col("ADMSN_HR")))
    lt = lt.withColumn("ADMSN_HR", F.when(F.col("ADMSN_HR") == '', "00").otherwise(F.col("ADMSN_HR")))
    lt = lt.withColumn("ADMSN_DT", F.concat(F.col("ADMSN_DT"), F.col("ADMSN_HR")))

    lt = lt.withColumn("admission_datetime", F.to_timestamp(F.col("ADMSN_DT"), "ddMMMyyyyHH"))
    return lt
