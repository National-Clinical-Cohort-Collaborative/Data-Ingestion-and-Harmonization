# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


@transform_df(
    Output(configs.transform + "03 - prepared/" + "rx_prepared"),
    rx_melted=Input("ri.foundry.main.dataset.a7ce3f8c-4347-4ec6-80be-3f17b3b92141"),
    rx_original=Input("ri.foundry.main.dataset.292106b5-cd0c-4a32-bc2d-6f9369ffac0b")
)
def compute(rx_melted, rx_original):
    rx_original_filtered = rx_original.select("pkey", "PRSCRBD_DT", "NDC_QTY", "NEW_RX_REFILL_NUM")

    rx_original_filtered = rx_original_filtered.withColumn("rx_refill_num", rx_original_filtered.NEW_RX_REFILL_NUM.cast("int"))

    # Convert all column names to lowercase
    for original_col_name in rx_original_filtered.columns:
        rx_original_filtered = rx_original_filtered.withColumnRenamed(original_col_name, original_col_name.lower())

    rx_final = rx_melted.join(rx_original_filtered, "pkey")

    return rx_final
