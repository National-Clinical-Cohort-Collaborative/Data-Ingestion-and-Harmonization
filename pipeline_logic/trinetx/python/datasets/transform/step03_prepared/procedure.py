from transforms.api import transform, Input, Output
from trinetx.utils import add_coalesced_code_cols, add_site_id_col, add_mapping_cols
from trinetx.site_specific_utils import apply_site_parsing_logic


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/03 - prepared/procedure'),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/02 - clean/procedure'),
    mapping_table=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/TriNetX Vocab Correction Mapping Table'),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 77'),
)
def compute_function(my_input, mapping_table, site_id_df, processed):

    processed_df = my_input.dataframe()

    # Created COALESCED_MAPPED_CODE_SYSTEM and COALESCED_MAPPED_CODE columns
    processed_df = add_coalesced_code_cols(
        processed_df,
        second_mapped_code_system_col='PX_CODE_SYSTEM',
        second_mapped_code_col='PX_CODE'
    )

    # Use mapping table to add columns with mapped values that will allow for joins on concept table
    # Added columns have PREPARED_ prefix
    domain = 'procedure'
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

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    processed.write_dataframe(processed_df)
