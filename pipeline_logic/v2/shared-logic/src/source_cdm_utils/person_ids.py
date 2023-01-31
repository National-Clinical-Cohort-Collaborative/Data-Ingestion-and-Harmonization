from pyspark.sql import functions as F, Window as W
from transforms.verbs import dataframes as D


def person_id_pre_clean(ctx, person, **input_dfs):
    pairs_to_union = []
    persons_to_union = []

    for (alias, df) in input_dfs.items():
        # Get person_ids for "missing persons" check
        persons_to_union.append(df.select("person_id", "data_partner_id"))

        # If the domain has a person_id and visit_occurrence_id column
        if alias in domains_to_filter_multiple_persons_per_visit:
            # Get person_id + visit_occurrence_id pairs for "multiple persons per visit" check
            pairs_to_union.append(df.select("person_id", "visit_occurrence_id", "data_partner_id"))

    distinct_pairs = D.union_many(*pairs_to_union).distinct()
    all_persons = D.union_many(*persons_to_union).distinct()

    # DETERMINE ROWS TO REMOVE + CREATE UNIFORM SCHEMA
    # Rows with multiple persons per visit
    multiple_persons_df = get_rows_with_multiple_persons_per_visit(distinct_pairs) \
        .select("person_id",
                "visit_occurrence_id",
                "data_partner_id",
                "removal_reason")

    # Rows with person_ids not found in the person table
    # NOTE that this will report 'null' as a missing person if null person_ids exist.
    missing_persons_df = get_rows_with_missing_persons(all_persons, person) \
        .select("person_id",
                "data_partner_id",
                "removal_reason") \
        .withColumn("visit_occurrence_id", F.lit(None))

    bad_person_ids = multiple_persons_df.unionByName(missing_persons_df)

    return bad_person_ids


domains_to_filter_multiple_persons_per_visit = [
    "condition_occurrence",
    "procedure_occurrence",
    "measurement",
    "observation",
    "visit_occurrence",
    "drug_exposure",
    "device_exposure"
]


# Return rows that have a visit_occurrence_id associated with more than 1 person_id
def get_rows_with_multiple_persons_per_visit(domain_df):
    w = W.partitionBy("visit_occurrence_id")
    # collect all rows that have a visit_occurrence_id associated with more than 1 person_id
    bad_rows_df = domain_df.filter(F.col('visit_occurrence_id').isNotNull()) \
        .withColumn('distinct_person_count', F.size(F.collect_set("person_id").over(w)))
    bad_rows_df = bad_rows_df \
        .filter(bad_rows_df.distinct_person_count > 1) \
        .drop('distinct_person_count') \
        .withColumn('removal_reason', F.lit('MULTIPLE_PERSONS_PER_VISIT_OCCURRENCE'))

    return bad_rows_df


# Return rows that have a person_id not found in the person domain
# REQUIRES that both {all_persons_df} and {person_domain_df} have person_id column
# NOTE that this will report 'null' as a missing person if null person_ids exist.
def get_rows_with_missing_persons(all_persons_df, person_domain_df):
    bad_rows_df = all_persons_df.join(person_domain_df, on='person_id', how='left_anti')

    return bad_rows_df.withColumn('removal_reason', F.lit('PERSON_ID_NOT_IN_PERSON_DOMAIN'))
