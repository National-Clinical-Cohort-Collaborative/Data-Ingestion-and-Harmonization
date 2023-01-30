from pyspark.sql import types as T, functions as F
from transforms.api import transform_df, Input, Output
from pedsnet.anchor import path


@transform_df(
    Output(path.metadata + "payload_status"),
    parsed_df=Input(path.transform + "00 - unzipped/payload_filename"),
    passed_pipeline_df=Input(path.union_staging + "staged/" + "person"),
    site_id=Input(path.site_id)
)
def my_compute_function(ctx, parsed_df, passed_pipeline_df, site_id):
    # parsed_df tells us the payload currently being processed in the transformation pipeline
    try:
        parsed_payload = parsed_df.where(F.col("newest_payload") == True).head().payload
    except IndexError:
        parsed_payload = "Payload info not available"

    # passed_pipeline_df tells us the payload that has successfully made it through the pipeline
    try:
        passed_pipeline_payload = passed_pipeline_df.select("payload").distinct().head().payload
    except IndexError:
        passed_pipeline_payload = "Payload info not available"

    data_partner_id = int(site_id.head().data_partner_id)

    schema = T.StructType([
        T.StructField('parsed_payload', T.StringType()),
        T.StructField('unreleased_payload', T.StringType()),
        T.StructField('data_partner_id', T.IntegerType())
    ])

    df_out = ctx.spark_session.createDataFrame(
        data=[(parsed_payload, passed_pipeline_payload, data_partner_id)],
        schema=schema
    )

    return df_out
