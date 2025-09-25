from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from transforms.verbs import dataframes as D
from cms import configs

inputs = {ds: Input(configs.transform + "01 - parsed/errors/errors_" + ds) for ds in configs.input_file_list}


@transform_df(
    Output(configs.metadata + "errors_union"),
    **inputs
)
def compute(**input_dfs):
    selected_dfs = list(df.withColumn("source_table", F.lit(name)) for name, df in input_dfs.items())
    unioned_df = D.union_many(*selected_dfs)
    return unioned_df.select("source_table", "row_number", "error_type", "error_details")
