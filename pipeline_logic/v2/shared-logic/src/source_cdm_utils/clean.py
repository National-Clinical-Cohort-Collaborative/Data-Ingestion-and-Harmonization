from pyspark.sql import functions as F


empty_payer_plan_cols = [
    "payer_source_concept_id",
    "plan_concept_id",
    "plan_source_concept_id",
    "sponsor_concept_id",
    "sponsor_source_concept_id",
    "stop_reason_concept_id",
    "stop_reason_source_concept_id"
]


def conceptualize(domain, df, concept):
    concept = concept.select("concept_id", "concept_name")

    concept_id_columns = [col for col in df.columns if col.endswith("_concept_id")]
    # _concept_id columns should be Integer
    df = df.select(
        *[col for col in df.columns if col not in concept_id_columns] +
        [F.col(col).cast("integer").alias(col) for col in concept_id_columns]
    )

    for col in concept_id_columns:
        new_df = df

        if col in empty_payer_plan_cols:
            # Create an empty *_concept_name column for these cols to prevent an OOM during the join while keeping the schema consistent
            new_df = new_df.withColumn("concept_name", F.lit(None).cast("string"))
        else:
            new_df = new_df.join(concept, [new_df[col] == concept["concept_id"]], "left_outer").drop("concept_id")

        concept_type = col[:col.index("_concept_id")]
        new_df = new_df.withColumnRenamed("concept_name", concept_type+"_concept_name")

        df = new_df

    # occurs in observation, measurement
    if "value_as_number" in df.columns:
        df = df.withColumn("value_as_number", df.value_as_number.cast("double"))

    if "person_id" in df.columns:
        df = df.filter(df.person_id.isNotNull())

    return df
