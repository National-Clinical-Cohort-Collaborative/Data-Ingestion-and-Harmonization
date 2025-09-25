from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from cms import configs

output_checks = [Check(E.primary_key('pkey'), 'pkey is unique', on_error='FAIL')]


def make_transform(dataset):
    @transform_df(
        Output(configs.transform + "02 - schema applied/" + dataset.name, checks=output_checks),
        input_df=Input(configs.transform + "01 - union/" + dataset.name)
    )
    def compute_function(input_df):
        prepped_columns = [F.col(col) for col in dataset.pkey_columns]
        result_df = input_df.select(F.xxhash64(*prepped_columns).cast("string").alias("pkey"), "*")
        return result_df

    return compute_function


transforms = (make_transform(dataset) for dataset in configs.datasets)
