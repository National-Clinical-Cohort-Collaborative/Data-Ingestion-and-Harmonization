from transforms.api import transform_df, Input, Output
from datetime import datetime
from pyspark.sql import functions as F
from pedsnet import local_schemas
from pedsnet.anchor import path
from source_cdm_utils import schema


@transform_df(
    Output(path.metadata + "manifest"),
    manifest_df=Input(path.transform + "01 - parsed/metadata/manifest"),
    site_id_df=Input(path.site_id),
    data_partner_ids=Input(path.all_ids),
    omop_vocab=Input(path.vocab),
    control_map=Input(path.metadata + "control_map")
)
def my_compute_function(ctx, manifest_df, site_id_df, data_partner_ids, omop_vocab, control_map):
    # Handle empty manifest
    if manifest_df.count() == 0:
        schema_dict = local_schemas.manifest_schema
        schema_struct = schema.schema_dict_to_struct(schema_dict, False)
        data = [["[No manifest provided]" for _ in schema_dict]]
        processed_df = ctx.spark_session.createDataFrame(data, schema_struct)

        # Add CDM
        site_id_df = site_id_df.join(data_partner_ids, "data_partner_id", "left")
        try:
            cdm = site_id_df.head().source_cdm
            processed_df = processed_df.withColumn("CDM_NAME", F.lit(cdm))
        except IndexError:
            pass
    else:
        processed_df = manifest_df

    curr_date = datetime.date(datetime.now())
    processed_df = processed_df.withColumn("CONTRIBUTION_DATE", F.lit(curr_date).cast("date"))

    omop_vocab = omop_vocab.where(omop_vocab["vocabulary_id"] == "None").where(omop_vocab["vocabulary_name"] == "OMOP Standardized Vocabularies")
    vocabulary_version = omop_vocab.head().vocabulary_version
    processed_df = processed_df.withColumn("N3C_VOCAB_VERSION", F.lit(vocabulary_version).cast("string"))

    # Compute approximate expected person count using the CONTROL_MAP file
    # Approximate xpected person count = (# rows in CONTROL_MAP) / 2 * 3
    control_map_total_count = control_map.count()
    approx_expected_person_count = int((control_map_total_count / 2) * 3)
    processed_df = processed_df.withColumn("APPROX_EXPECTED_PERSON_COUNT", F.lit(approx_expected_person_count))

    return processed_df
