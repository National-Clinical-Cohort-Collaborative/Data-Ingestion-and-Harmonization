from pyspark.sql import types as T, functions as F

schema = T.StructType([
    T.StructField('parsed_payload', T.StringType()),
    T.StructField('unreleased_payload', T.StringType()),
    T.StructField('data_partner_id', T.IntegerType())
])


def payload_status(ctx, start_df, end_df, site_id):
    # start_df tells us the payload currently being processed in the transformation pipeline
    try:
        start_payload = start_df.where(F.col("newest_payload") == True).head().payload  # noqa
    except AttributeError:
        start_payload = "Payload info not available"

    # end_df tells us the payload that has successfully made it through the pipeline
    try:
        end_payload = end_df.select("payload").distinct().head().payload  # noqa
    except AttributeError:
        end_payload = "Payload info not available"

    data_partner_id = int(site_id.head().data_partner_id)  # noqa

    result = ctx.spark_session.createDataFrame(
        data=[(start_payload, end_payload, data_partner_id)],
        schema=schema
    )

    return result
