from transforms.api import transform, Input, Output
from trinetx.utils import add_coalesced_code_cols, add_site_id_col, add_mapping_cols
from trinetx.site_specific_utils import apply_site_parsing_logic
from trinetx.anchor import path


def make_transform(domain, col, system_col):
    @transform(
        processed=Output(path.transform + '03 - prepared/' + domain),
        my_input=Input(path.transform + '02 - clean/' + domain),
        mapping_table=Input(path.mapping),
        site_id_df=Input(path.site_id)
    )
    def compute_function(my_input, mapping_table, site_id_df, processed):
        processed_df = my_input.dataframe()
        mapping_df = mapping_table.dataframe()

        # Create COALESCED_MAPPED_CODE_SYSTEM and COALESCED_MAPPED_CODE columns
        if system_col and col:
            processed_df = add_coalesced_code_cols(
                processed_df,
                second_mapped_code_system_col=system_col,
                second_mapped_code_col=col
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

    return compute_function


domains = [
    ("adt", None, None),
    ("control_map", None, None),
    ("diagnosis", "dx_code", "dx_code_system"),
    ("lab_result", "lab_code", "lab_code_system"),
    ("medication", "rx_code", "rx_code_system"),
    ("note", None, None),
    ("note_nlp", None, None),
    ("patient", None, None),
    ("procedure", "px_code", "px_code_system"),
    ("vital_signs", "vital_code", "vital_code_system")
]

transforms = (make_transform(*domain) for domain in domains)
