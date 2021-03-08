from transforms.api import transform, Input, Output
from myproject.parsing import parse_input
from myproject.utils import get_site_id
import pyspark.sql.functions as F


domain = "care_site"
regex = "(?i).*CARE_SITE.*"


@transform(
    processed=Output("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/01 - parsed/care_site"),
    payload=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/00 - unzipped/payload_filename"),
    errors=Output("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/01 - parsed/errors/care_site"),
    my_input=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/00 - unzipped/unzipped_raw_data"),
    site_id_df=Input("/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 25")
)
def compute_function(ctx, my_input, payload, site_id_df, processed, errors):
    """
    By default, the parse_input function will use a csv file processing function found in parsing.py
    (Default parameters: delimiter = "|")

    Any rows that are formatted incorrectly, e.g. do not have the appropriate number of fields for the
    given domain schema, will be read into the 'errors' dataset.
    """

    site_id = get_site_id(site_id_df)
    processed_df = parse_input(ctx, my_input, errors, site_id, domain, regex)
    payload_filename = payload.dataframe().where(F.col("newest_payload") == True).take(1)[0].payload
    processed_df = processed_df.withColumn("payload", F.lit(payload_filename))
    processed.write_dataframe(processed_df)
