from transforms.api import transform, Input, Output, Check
from pyspark.sql.functions import col, coalesce
from trinetx.utils import add_site_id_col, add_mapping_cols
from transforms import expectations as E
from trinetx.site_specific_utils import apply_site_parsing_logic


"""
If the input dataset for this transform (coming from step 02) has invalid primary keys,
the health checks from the previous step will issue a warning. During this step, rows
with null values for primary key columns will be dropped.

For the encounter table, there may be "duplicate" rows that represent the same encounter
and have the same encounter_id but have two different mapped encounter types because the source
encounter type mapped to two different codes. We handle these records in step 4 (combining
the two rows into one row).
"""


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/encounter'),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/02 - clean/encounter'),
    mapping_table=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/TriNetX Vocab Correction Mapping Table'),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 77'),
)
def compute_function(my_input, mapping_table, site_id_df, processed):

    processed_df = my_input.dataframe()

    processed_df = processed_df.withColumn('COALESCED_MAPPED_ENCOUNTER_TYPE', coalesce(col("MAPPED_ENCOUNTER_TYPE"), col('ENCOUNTER_TYPE')))

    # Use mapping table to add columns with mapped values that will allow for joins on concept table
    # Added columns have PREPARED_ prefix
    domain = 'encounter'
    mapping_df = mapping_table.dataframe()
    mapping_df = mapping_df.filter(mapping_df.trinetx_domain == domain)
    # Create prepared columns for all columns listed for this domain in the mapping table
    columns = [i.trinetx_domain_column for i in mapping_df.select('trinetx_domain_column').distinct().collect()]
    processed_df = add_mapping_cols(
        processed_df,
        mapping_df,
        site_id_df,
        domain=domain,
        columns=columns
    )

    # Add a column with the site id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Drop any records with a null primary key (possibly due to casting step)
    processed_df = processed_df.where(col("ENCOUNTER_ID").isNotNull())

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    processed.write_dataframe(processed_df)
