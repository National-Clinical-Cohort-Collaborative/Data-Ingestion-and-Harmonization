from pyspark.sql import functions as F


def get_domain_id(df, concept_df, concept_col):

    # Filtering to standard concepts only
    filtered_concept_df = concept_df.select(
        "concept_id",
        "standard_concept",
        "domain_id"
    ).where(
        F.col("standard_concept") == F.lit("S")
    ).drop("standard_concept")

    # Pull in domain ids
    df = df.join(
        filtered_concept_df,
        on=(df[concept_col] == filtered_concept_df["concept_id"]),
        how="left"
    ).drop("concept_id")

    return df
