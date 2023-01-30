from pyspark.sql import functions as F


# Map source concept_ids in concept_col to standard concepts
# Use concept and concept_relationship tables to fill out source/target concept_id info
def perform_standard_mapping(df, concept_df, concept_relationship_df, concept_col):
    original_cols = [col for col in df.columns if col]  # Handle site 1015's empty column

    # Map source concept_ids to standard concept_ids when possible
    # Note that standard concept_ids map to themselves in the concept_relationship table
    df = df.join(
        concept_relationship_df,
        on=(
            (df[concept_col] == concept_relationship_df["concept_id_1"]) &
            (F.lower(concept_relationship_df["relationship_id"]) == "maps to") &
            (concept_relationship_df["invalid_reason"].isNull())
        ),
        how="left"
    ).withColumn("source_concept_id", df[concept_col])\
     .withColumnRenamed("concept_id_2", "target_concept_id")

    # Bring in relevant concept info for source concepts
    df = df.join(
        concept_df,
        (df["source_concept_id"] == concept_df["concept_id"]),
        "left"
    )
    df = df.selectExpr(
        *original_cols,
        "source_concept_id",
        "target_concept_id",
        "concept_name as source_concept_name",
        "domain_id as source_domain_id",
        "vocabulary_id as source_vocabulary_id",
        "concept_class_id as source_concept_class_id",
        "standard_concept as source_standard_concept"
    )
    cols_to_keep = df.columns

    # Bring in relevant concept info for target concepts
    df = df.join(
        concept_df,
        (df["target_concept_id"] == concept_df["concept_id"]),
        "left"
    )
    df = df.selectExpr(
        *cols_to_keep,
        "concept_name as target_concept_name",
        "domain_id as target_domain_id",
        "vocabulary_id as target_vocabulary_id",
        "concept_class_id as target_concept_class_id",
        "standard_concept as target_standard_concept"
    )

    return df
