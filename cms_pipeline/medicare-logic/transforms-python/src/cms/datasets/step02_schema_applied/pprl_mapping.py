from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


@transform_df(
    Output(configs.transform + "02 - schema applied/pprl_mapping"),
    source_df=Input(configs.transform + "01 - parsed/processed/pprl_mapping"),
)
def compute(source_df):
    # Casting to string instead of int so that stats look better
    source_df = source_df.withColumn("group", F.col("gpi").cast("string"))

    source_df = source_df.withColumn("split", F.split("pseudo_id", ":"))
    source_df = source_df.withColumn("institution", F.col("split").getItem(0))
    source_df = source_df.withColumn("site_person_id", F.col("split").getItem(1))

    return source_df.select("institution", "site_person_id", "group")
