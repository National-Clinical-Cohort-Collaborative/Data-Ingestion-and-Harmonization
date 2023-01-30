from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from trinetx.anchor import path

required_domain_list = ["diagnosis", "encounter", "lab_result", "medication", "patient", "procedure"]
optional_domain_list = ["vital_signs"]

required_domains = {domain: Input(path.transform + "01 - parsed/required/" + domain) for domain in required_domain_list}
optional_domains = {domain: Input(path.transform + "01 - parsed/optional/" + domain) for domain in optional_domain_list}


@transform_df(
    Output(path.metadata + "data_counts_check"),
    raw_counts=Input(path.transform + "01 - parsed/metadata/data_counts"),
    **required_domains,
    **optional_domains
)
def my_compute_function(ctx, raw_counts, **domains):
    data = []
    for domain_name, domain_df in domains.items():
        row_count = domain_df.count()
        data.append((domain_name.lower(), row_count))

    # Create dataframe with row counts for each domain
    df = ctx.spark_session.createDataFrame(data, ['domain', 'parsed_row_count'])

    # Join in row counts from DATA_COUNT csv
    for col_name in raw_counts.columns:
        raw_counts = raw_counts.withColumnRenamed(col_name, col_name.upper())

    df = df.join(raw_counts, df.domain == F.lower(raw_counts.TABLE_NAME), 'left')
    df = df.withColumn("delta_row_count", F.coalesce(df.ROW_COUNT, F.lit(0)) - F.coalesce(df.parsed_row_count, F.lit(0)))
    df = df.selectExpr("domain", "cast(ROW_COUNT as long) as loaded_row_count", "parsed_row_count", "delta_row_count")

    return df
