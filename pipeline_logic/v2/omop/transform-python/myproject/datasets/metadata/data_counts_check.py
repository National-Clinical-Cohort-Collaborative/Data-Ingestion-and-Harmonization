from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from myproject.anchor import path


def domain_input(domain, folder):
    return Input(path.transform + "01 - parsed/" + folder + "/" + domain)


required_domain_list = [
    "condition_era", "condition_occurrence", "death", "drug_era", "drug_exposure", "location",
    "measurement", "observation", "observation_period", "person", "procedure_occurrence", "visit_occurrence"
]
optional_domain_list = ["care_site", "device_exposure", "dose_era", "provider"]
cached_domain_list = []

required_domains = dict((domain, domain_input(domain, "required")) for domain in required_domain_list)
optional_domains = dict((domain, domain_input(domain, "optional")) for domain in optional_domain_list)
cached_domains = dict((domain, domain_input(domain, "cached")) for domain in cached_domain_list)


@transform_df(
    Output(path.metadata + "data_counts_check"),
    raw_counts=Input(path.transform + "01 - parsed/metadata/data_counts"),
    **required_domains,
    **optional_domains,
    **cached_domains
)
def my_compute_function(ctx, raw_counts, **domains):
    data = []
    for domain_name, domain_df in domains.items():
        row_count = domain_df.count()
        data.append((domain_name, row_count))

    # Create dataframe with row counts for each domain
    df = ctx.spark_session.createDataFrame(data, ['domain', 'parsed_row_count'])

    # Join in row counts from DATA_COUNT csv
    for col_name in raw_counts.columns:
        raw_counts = raw_counts.withColumnRenamed(col_name, col_name.upper())

    df = df.join(raw_counts, df.domain == F.lower(raw_counts.TABLE_NAME), 'left')
    df = df.withColumn("delta_row_count", F.coalesce(df.ROW_COUNT, F.lit(0)) - F.coalesce(df.parsed_row_count, F.lit(0)))
    df = df.selectExpr("domain", "cast(ROW_COUNT as long) as loaded_row_count", "parsed_row_count", "delta_row_count")

    return df
