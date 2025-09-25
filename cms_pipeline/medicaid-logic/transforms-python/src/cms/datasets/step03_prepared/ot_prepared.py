from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


@transform_df(
    Output(configs.transform + "03 - prepared/" + "ot_prepared"),
    ot=Input("ri.foundry.main.dataset.7996f1b7-6f17-421a-b6a1-39aded1c3e4a"),
)
def compute(ot):
    ot = ot.withColumn("BILL_TYPE_CD_digit1", F.substring(ot.BILL_TYPE_CD, 1, 1))
    ot = ot.withColumn("BILL_TYPE_CD_digit2", F.substring(ot.BILL_TYPE_CD, 2, 1))
    ot = ot.withColumn("BILL_TYPE_CD_digit3", F.substring(ot.BILL_TYPE_CD, 3, 1))
    ot = ot.withColumn("BILL_TYPE_CD_digit4", F.substring(ot.BILL_TYPE_CD, 4, 1))

    return ot
