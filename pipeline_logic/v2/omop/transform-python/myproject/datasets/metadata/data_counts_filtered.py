from transforms.api import transform_df, Input, Output
from myproject.anchor import path


@transform_df(
    Output(path.metadata + "data_counts_filtered"),
    source_df=Input(path.metadata + "data_counts_check"),
)
def compute(source_df):
    return source_df.filter(source_df.delta_row_count != 0)
