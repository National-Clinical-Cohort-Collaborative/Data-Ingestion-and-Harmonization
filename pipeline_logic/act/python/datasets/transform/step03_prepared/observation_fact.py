from transforms.api import transform, Input, Output
from act.utils import add_site_id_col, split_concept_cd_col, create_mapped_vocab_codes_col
from act.site_specific_utils import apply_site_parsing_logic
from pyspark.sql.functions import col, concat_ws, coalesce


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/observation_fact'),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/02 - clean/observation_fact'),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 411'),
    local_code_map=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/metadata/act_local_code_map'),
    vocab_id_map=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/metadata/n3c_vocab_map"),
    vocab_id_map_overrides=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Reference Tables/Vocab ID Mapping/act_vocab_id_mapping_table")
)
def compute_function(my_input, site_id_df, local_code_map, vocab_id_map, vocab_id_map_overrides, processed):

    processed_df = my_input.dataframe()

    # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    # Create column for comparison with Adeptia data
    pkey_cols = ['encounter_num', 'patient_num', 'concept_cd', 'provider_id', 'start_date', 'modifier_cd', 'instance_num']
    processed_df = processed_df.withColumn("site_comparison_key", concat_ws("|", *[col(x) for x in pkey_cols]))

    # Add column for local codes that must be mapped to standard using site-provided ACT_LOCAL2STANDARD mapping file
    local_code_map = local_code_map.dataframe().select("local_concept_cd", "act_standard_code").distinct()
    processed_df = processed_df.join(
        local_code_map,
        (processed_df["concept_cd"] == local_code_map["local_concept_cd"]),
        "left"
    ).drop("local_concept_cd")
    processed_df = processed_df.withColumnRenamed("act_standard_code", "mapped_concept_cd")

    # Split concept_cd into a vocab code and a concept code
    # If there's a mapped_concept_cd, from the local code map, split that instead
    processed_df = split_concept_cd_col(
        processed_df,
        parsed_vocab_col_name="parsed_vocab_code",
        parsed_concept_code_name="parsed_concept_code"
    )
    # Map the vocab code to a value that's in the concept table
    vocab_id_map = vocab_id_map.dataframe()
    processed_df = create_mapped_vocab_codes_col(processed_df, vocab_id_map)
    # The site's vocab map isn't always reliable -- fix any mismapped codes
    vocab_id_map_overrides = vocab_id_map_overrides.dataframe()
    vocab_id_map_overrides = vocab_id_map_overrides.withColumnRenamed(
        "source_vocab_code", "parsed_vocab_code"
    ).withColumnRenamed(
        "mapped_vocab_code", "mapped_vocab_code_override"
    ).select("data_partner_id", "parsed_vocab_code", "mapped_vocab_code_override")
    processed_df = processed_df.join(
        vocab_id_map_overrides,
        ["data_partner_id", "parsed_vocab_code"],
        "left"
    ).withColumn(
        "mapped_vocab_code", coalesce("mapped_vocab_code_override", "mapped_vocab_code")
    )

    """
    observation_fact table now has the following derived columns:
    - mapped_concept_cd:    a mapped version of whatever was in concept_cd based on the ACT local codes
                            file provided by sites (only one site is using local codes as of 2021/02/12)
    - parsed_vocab_code:    split the concept_cd column (or the mapped_concept_cd if it's non-null) on the ":" character
                            and use the FIRST substring as the parsed_vocab_code
    - parsed_concept_code:  split the concept_cd column (or the mapped_concept_cd if it's non-null) on the ":" character
                            and use the SECOND substring as the parsed_concept_code
    - mapped_vocab_code:    as needed, replace the parsed_vocab_code with a vocabulary_id
                            that's in the OMOP concept table. If no mapping is needed, fill
                            this field with the normal parsed_vocab_code.
                            e.g. (parsed_vocab_code="ICD") ==> (mapped_vocab_code="ICD10CM") // replace using mapping table
                                 (parsed_vocab_code="RXNORM") ==> (mapped_vocab_code="RXNORM") // no change needed
    """
    processed.write_dataframe(processed_df)
