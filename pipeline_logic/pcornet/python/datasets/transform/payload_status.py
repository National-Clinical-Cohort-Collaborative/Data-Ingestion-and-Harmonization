from transforms.api import transform_df, Input, Output
from pyspark.sql import types as T
from pyspark.sql import functions as F


@transform_df(
    Output("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/metadata/payload_status"),
    parsed_df=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/00 - unzipped/payload_filename"),
    passed_pipeline_df=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/person"),
    site_id=Input("/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 793")
)
def my_compute_function(ctx, parsed_df, passed_pipeline_df, site_id):
    # parsed_df tells us the payload currently being processed in the transformation pipeline
    parsed_payload = parsed_df.where(F.col("newest_payload") == True).take(1)[0].payload
    # passed_pipeline_df tells us the payload that has successfully made it through the pipeline
    passed_pipeline_payload = passed_pipeline_df.select("payload").distinct().take(1)[0].payload
    data_partner_id = int(site_id.take(1)[0].data_partner_id)

    schema = T.StructType([
        T.StructField('parsed_payload', T.StringType(), True),
        T.StructField('unreleased_payload', T.StringType(), True),
        T.StructField('data_partner_id', T.IntegerType(), True)
    ])

    df_out = ctx.spark_session.createDataFrame(
        data=[(parsed_payload, passed_pipeline_payload, data_partner_id)],
        schema=schema
    )

    return df_out
