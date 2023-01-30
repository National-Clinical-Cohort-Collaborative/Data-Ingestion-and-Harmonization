from transforms.api import transform, Input, Output
from pyspark.sql.functions import col, coalesce
from trinetx.utils import add_site_id_col, add_mapping_cols
from trinetx.site_specific_utils import apply_site_parsing_logic
from trinetx.anchor import path

domain = 'encounter'


@transform(
    processed=Output(path.transform + '03 - prepared/' + domain),
    my_input=Input(path.transform + '02 - clean/' + domain),
    mapping_table=Input(path.mapping),
    site_id_df=Input(path.site_id),
)
def compute_function(my_input, mapping_table, site_id_df, processed):
    processed_df = my_input.dataframe()
    mapping_df = mapping_table.dataframe()

    # System columns are uppercase
    processed_df = processed_df.withColumn(
        'COALESCED_MAPPED_ENCOUNTER_TYPE',
        coalesce(col("mapped_encounter_type"), col('encounter_type'))
    )

    # Use mapping table to add columns with mapped values that will allow for joins on concept table
    # Added columns have PREPARED_ prefix
    mapping_df = mapping_df.filter(mapping_df.trinetx_domain == domain)

    # Create prepared columns for all columns listed for this domain in the mapping table
    columns = [i.trinetx_domain_column for i in mapping_df.select('trinetx_domain_column').distinct().collect()]  # noqa
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
