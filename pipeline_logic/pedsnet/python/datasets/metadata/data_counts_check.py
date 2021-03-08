from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Explicitly list each domain so that the template modifies the filepath when deployed to other sites:
domains = {
    "care_site": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/care_site"),
    "condition_era": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/condition_era"),
    "condition_occurrence": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/condition_occurrence"),
    "death": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/death"),
    "dose_era": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/dose_era"),
    "drug_era": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/drug_era"),
    "drug_exposure": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/drug_exposure"),
    "location": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/location"),
    "measurement": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/measurement"),
    "observation": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/observation"),
    "person": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/person"),
    "procedure_occurrence": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/procedure_occurrence"),
    "provider": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/provider"),
    "visit_occurrence": Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/01 - parsed/visit_occurrence"),
}


@transform_df(
    Output(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/metadata/data_counts_check",
        checks=[
            Check(E.count().gt(0), 'Valid data counts file provided', on_error='WARN'),
            Check(E.col('delta_row_count').equals(0), 'Parsed row count equals loaded row count', on_error='WARN'),
        ]
    ),
    raw_counts=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/metadata/data_counts_parsed"),
    **domains
)
def my_compute_function(raw_counts, ctx, **domains):

    data = []
    for domain_name, domain_df in domains.items():
        row_count = domain_df.count()
        data.append((domain_name, row_count))

    # Create dataframe with row counts for each domain
    df = ctx.spark_session.createDataFrame(
        data,
        ['domain', 'parsed_row_count']
    )

    try:
        # Join in row counts from DATA_COUNT csv
        for col_name in raw_counts.columns:
            raw_counts = raw_counts.withColumnRenamed(col_name, col_name.upper())
        df = df.join(raw_counts, df.domain == F.lower(raw_counts.TABLE_NAME), 'left')
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

