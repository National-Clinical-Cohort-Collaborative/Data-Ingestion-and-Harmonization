from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


def make_transform(dataset):
    @transform_df(
        Output(configs.transform + "02 - schema applied/" + dataset.name),
        input_df=Input(configs.transform + "01 - parsed/processed/" + dataset.name)
    )
    def compute_function(input_df):
        prepped_columns = [F.col(col.upper()) for col in dataset.pkey_columns]
        if "tax_num" in dataset.pkey_columns:
            input_df = input_df.withColumn("TAX_NUM_ID", F.xxhash64(input_df.TAX_NUM))
        result_df = input_df.select(F.xxhash64(*prepped_columns).cast("string").alias("pkey"), "*")
        return result_df

    return compute_function


transforms = (make_transform(dataset) for dataset in configs.datasets)
