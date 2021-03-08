from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from pyspark.sql import functions as F
from pyspark.sql import types as T

domains = {
    'CONDITION': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/condition'),
    'DEATH': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/death'),
    'DEATH_CAUSE': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/death_cause'),
    'DEMOGRAPHIC': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/demographic'),
    'DIAGNOSIS': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/diagnosis'),
    'DISPENSING': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/dispensing'),
    'ENCOUNTER': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/encounter'),
    'IMMUNIZATION': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/immunization'),
    'LAB_RESULT_CM': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/lab_result_cm'),
    'LDS_ADDRESS_HISTORY': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/lds_address_history'),
    'MED_ADMIN': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/med_admin'),
    'OBS_CLIN': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/obs_clin'),
    'OBS_GEN': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/obs_gen'),
    'PRESCRIBING': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/prescribing'),
    'PROCEDURES': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/procedures'),
    'PROVIDER': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/provider'),
    'PRO_CM': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/pro_cm'),
    'VITAL': Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/vital'),
}


@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/metadata/data_counts_check",
        checks=[
            Check(E.count().gt(0), 'Valid data counts file provided', on_error='WARN'),
            Check(E.col('delta_row_count').equals(0), 'Parsed row count equals loaded row count', on_error='WARN'),
        ]
    ),
    site_counts=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/metadata/data_counts_parsed"),
    **domains
)
def my_compute_function(ctx, site_counts, **domains):

    data = []
    for domain_name, domain_df in domains.items():
        row_count = domain_df.count()
        data.append((domain_name.lower(), row_count))

    # Create dataframe with row counts for each domain
    df = ctx.spark_session.createDataFrame(
        data,
        ['domain', 'parsed_row_count']
    )

    try:
        # Join in row counts from DATA_COUNT csv
        for col_name in site_counts.columns:
            site_counts = site_counts.withColumnRenamed(col_name, col_name.upper())
        df = df.join(site_counts, df.domain == F.lower(site_counts.TABLE_NAME), 'left')
        df = df.withColumn("delta_row_count", df.ROW_COUNT - df.parsed_row_count)
        df = df.selectExpr("domain", "cast(ROW_COUNT as long) as loaded_row_count", "parsed_row_count", "delta_row_count")

    except:
        schema = T.StructType([
            T.StructField("domain", T.StringType(), True),
            T.StructField("loaded_row_count", T.LongType(), True),
            T.StructField("parsed_row_count", T.LongType(), True),
            T.StructField("delta_row_count", T.DoubleType(), True),
        ])
        df = ctx.spark_session.createDataFrame([], schema)

    return df
