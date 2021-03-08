from transforms.api import transform, Input, Output
from pcornet.utils import add_site_id_col, add_mapped_vocab_code_col, create_datetime_col
from pcornet.site_specific_utils import apply_site_parsing_logic
from pyspark.sql import functions as F


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/obs_clin'),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/02 - clean/obs_clin'),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 793'),
    mapping_table=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Reference Tables/Vocab ID Mapping/pcornet_vocab_id_mapping_table')
)
def compute_function(my_input, site_id_df, mapping_table, processed):

    processed_df = my_input.dataframe()

    # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    # Handle PCORnet 5.0 schema (containing obsclin_date and obsclin_time columns)
    if "obsclin_date" in processed_df.columns:
        processed_df = processed_df.withColumnRenamed("obsclin_date", "obsclin_start_date")
        processed_df = processed_df.withColumnRenamed("obsclin_time", "obsclin_start_time")
        processed_df = processed_df.withColumn("obsclin_stop_date", F.lit(None).cast("date"))
        processed_df = processed_df.withColumn("obsclin_stop_time", F.lit(None).cast("string"))
    # Data now conforms to PCORnet 6.0 schema (containing obsclin_start_date, obsclin_start_time, obsclin_stop_date, obsclin_stop_time columns)

    processed_df = create_datetime_col(processed_df, "obsclin_start_date", "obsclin_start_time", "OBSCLIN_START_DATETIME")
    processed_df = create_datetime_col(processed_df, "obsclin_stop_date", "obsclin_stop_time", "OBSCLIN_STOP_DATETIME")

    # Map the vocab code in source_vocab_col to a vocabulary_id that appears in the concept table using the mapping spreadsheet
    mapping_table = mapping_table.dataframe()
    processed_df = add_mapped_vocab_code_col(
        processed_df, mapping_table,
        domain="OBS_CLIN",
        source_vocab_col="obsclin_type",
        mapped_col_name="mapped_obsclin_type"
    )

    processed.write_dataframe(processed_df)
