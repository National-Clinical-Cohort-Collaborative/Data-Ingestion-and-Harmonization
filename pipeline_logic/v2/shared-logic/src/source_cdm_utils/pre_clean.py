"""
This file is the heart of the cleaning steps, but has become quite bloated over time. Should be re-written.
- tschwab, July 2022

updated by mchoudhury Oct 2022
"""


from pyspark.sql import functions as F, Window as W
from transforms.verbs import dataframes as D

DOMAIN_PKEYS = {
    "care_site": "care_site_id",
    "condition_era": "condition_era_id",
    "condition_occurrence": "condition_occurrence_id",
    "control_map": "control_map_id",
    "death": "person_id",
    "dose_era": "dose_era_id",
    "drug_era": "drug_era_id",
    "drug_exposure": "drug_exposure_id",
    "device_exposure": "device_exposure_id",
    "location": "location_id",
    "measurement": "measurement_id",
    "note": "note_id",
    "note_nlp": "note_nlp_id",
    "observation": "observation_id",
    "observation_period": "observation_period_id",
    "payer_plan_period": "payer_plan_period_id",
    "person": "person_id",
    "procedure_occurrence": "procedure_occurrence_id",
    "provider": "provider_id",
    "visit_detail": "visit_detail_id",
    "visit_occurrence": "visit_occurrence_id"
}

possible_phi_cols = {
    "condition_occurrence": [
        "condition_source_value",
        "condition_status_source_value"
    ],
    "death": [
        "cause_source_value"
    ],
    "measurement": [
        "value_source_value",
        "measurement_source_value"
    ],
    "observation": [
        "value_as_string",
        "observation_source_value",
    ],
    "person": [
        "gender_source_value",
        "race_source_value",
        "ethnicity_source_value",
    ],
    "procedure_occurrence": [
        "procedure_source_value"
    ],
    "provider": [
        "specialty_source_value",
        "gender_source_value"
    ],
    "visit_occurrence": [
        "visit_source_value",
        "admitting_source_value",
        "discharge_to_source_value",
    ]
}


def clean_free_text_cols(input_df, domain, p_key, ctx):
    domain = domain.lower()
    phi_cols = possible_phi_cols.get(domain, [])
    final_domain_df = input_df
    final_nulled_rows_df = None

    # Only apply this cleaning logic to domains with possible PHI columns
    if phi_cols:
        # Reduce the domain to only 2 identifying columns and the possible phi columns
        reduced_input_df = input_df.select(
            p_key,
            "data_partner_id",
            *phi_cols)

        # For efficiency, use lists to store dataframes that need to be unioned/joined after the 'for' loop
        nulled_rows_dfs = []
        preserve_rows_dfs = []
        # TODO Improvement: partitioning in a for loop like this (especially if it happens multiple times) causes lots of shuffling
        for col in phi_cols:
            # Reduce to only 2 identifying columns and the particular column we are investigating.
            # We can filter to non-null values because we will never mark a null column as possible PHI.
            df = reduced_input_df \
                .select(p_key,
                        "data_partner_id",
                        col) \
                .where(F.col(col).isNotNull())

            # (1) Create flag for records >= 60 characters in length
            df = df.withColumn("length_flag", F.length(df[col]) >= 60)
            # (2) Create flag for any non-numeric values with a frequency <= 2
            # Get value frequencies
            # w = W.partitionBy(col)
            df = df.withColumn('free_text_val_freq', F.lit(3))  # F.count('*').over(w))
            # Define conditions
            freq_cond = df['free_text_val_freq'] <= 2
            non_numeric_cond = df[col].rlike(".*[a-zA-Z]+.*")  # entry contains any letters
            # Create flags
            df = df \
                .withColumn("non_numeric_flag", non_numeric_cond) \
                .withColumn("non_numeric_infrequent_flag", freq_cond & non_numeric_cond) \
                .drop('free_text_val_freq')

            # (3) Create flag for records that match a PHI regex expression for personally-identifying prefixes
            regex_check = "Mr\.|Mrs\.|\bMiss\b|Dr\.|, M\.?D\.?"
            df = df.withColumn("regex_flag",
                               F.when(~df.non_numeric_flag, F.lit(False))  # All characters are numeric
                                .otherwise(df[col].rlike(regex_check)))

            # Log the rows that were flagged for this column (ie: BAD values)
            nulled_rows_df = df \
                .where(
                    (df["length_flag"] == True) |
                    (df["non_numeric_infrequent_flag"] == True) |
                    (df["regex_flag"] == True)) \
                .withColumn("column", F.lit(col)) \
                .withColumn("nulled_value", F.col(col)) \
                .select(
                    "data_partner_id",
                    "column",
                    "nulled_value",
                    "length_flag",
                    "non_numeric_infrequent_flag",
                    "regex_flag"
                )
            nulled_rows_dfs.append(nulled_rows_df)

            # Log the rows that were kept for this column (ie: GOOD values)
            preserve_rows_df = df \
                .where(
                    (df["length_flag"] == False) &
                    (df["non_numeric_infrequent_flag"] == False) &
                    (df["regex_flag"] == False)) \
                .select(
                    p_key,
                    col
                )
            preserve_rows_dfs.append(preserve_rows_df)

        # Union all nulled rows
        final_nulled_rows_df = D.union_many(*nulled_rows_dfs, spark_session=ctx.spark_session)

        # Update input_df to return
        # Remove all possible PHI columns from the dataframe
        final_domain_df = input_df \
            .drop(*phi_cols)
        # Join back in the possible PHI columns based ONLY on the preserved rows
        for df in preserve_rows_dfs:
            final_domain_df = final_domain_df \
                .join(df,
                      on=p_key,
                      how='left')

    return (final_domain_df, final_nulled_rows_df)


# Utility function that converts a dataset into a dictionary of {value}:{removal_reason} grouped by {col_name} to
# use as input parameter for get_bad_rows_by_column_val.
# REQUIRES bad_vals_df schema {col_name:string, value:int or string, removal_reason:string}
# RETURNS dictionary of form {col_name : {value : removal_reason, ...}, ...}
def get_bad_vals_dict(bad_vals_df):
    bad_vals_df = bad_vals_df \
        .groupBy('col_name').agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct('value', 'removal_reason'))).alias('val_reason_dict'))
    # Create dictionary
    bad_vals_dict = bad_vals_df.rdd.map(lambda x: (x[0], x[1])).collectAsMap()  # noqa
    return bad_vals_dict


# Utility function that identifies bad rows is {domain_df} if {col_name}'s value is in {bad_vals_dict}.keys()
# Expects bad_vals_dict to be a dict of form {value : removal_reason}
# Returns filtered {bad_rows_df}, which has the same schema as domain_df + 'removal_reason' column
def get_bad_rows_by_column_val(domain_df, col_name, bad_vals_dict):
    # Determine bad rows
    bad_rows_df = domain_df \
        .filter(F.col(col_name).isin(*bad_vals_dict.keys())) \
        .withColumn('removal_reason', get_value(bad_vals_dict)(F.col(col_name)))

    return bad_rows_df


def get_value(dictionary):
    def f(x):
        return dictionary.get(x)
    return F.udf(f)


def clear_person_source_value(df):
    df = df.withColumn("person_source_value", F.lit(None).cast("string"))
    return df


# Per NCATS request, for any records in the person table with a race_concept_id = 8657 ("American Indian or Alaska Native"),
# re-map to a race_concept_id = 45878142 ("Other").
# Also, for these rows, replace race_source_value with "Other" and race_source_concept_id with 0.
# tschwab update August 2022 - NCATS asked us to undo this hiding logic
def filter_anai_records(person_df):
    return person_df

    anai_race_concept_id = 8657             # Standard concept: concept_name = "American Indian or Alaska Native", domain_id = "Race"
    other_standard_concept_id = 45878142    # Standard concept: concept_name = "Other", domain_id = "Meas Value"

    person_df = person_df.withColumn(
        "anai_flag",
        F.when(F.col("race_concept_id") == F.lit(anai_race_concept_id), F.lit(1))\
        .otherwise(F.lit(0))
    ).withColumn(
        "race_concept_id",
        F.when(F.col("anai_flag") == F.lit(1), F.lit(other_standard_concept_id))\
        .otherwise(F.col("race_concept_id"))
    ).withColumn(
        "race_source_concept_id",
        F.when(F.col("anai_flag") == F.lit(1), F.lit(0))\
        .otherwise(F.col("race_source_concept_id"))
    ).withColumn(
        "race_source_value",
        F.when(F.col("anai_flag") == F.lit(1), F.lit("Other"))\
        .otherwise(F.col("race_source_value"))
    )

    # Handle records with AN/AI source values that aren't mapped to the AN/AI concept_id
    person_df = person_df.withColumn(
        "race_source_value",
        F.when(F.col("race_source_value").rlike("(?i)(.*indian.*native.*)|(.*indian.*alask.*)|(.*amer.*indian.*)|(.*choctaw.*indian.*)"), F.lit(None))\
        .otherwise(F.col("race_source_value"))
    )
    return person_df.drop("anai_flag")


def do_pre_clean(
    domain,
    processed, nulled_rows, removed_rows,
    foundry_df, removed_person_ids,
    ahrq_xwalk, tribal_zips, loincs_to_remove, ctx
):

    # Convert transform inputs to dataframes
    foundry_df, removed_person_ids, ahrq_xwalk, tribal_zips, loincs_to_remove = \
        foundry_df.dataframe(), removed_person_ids.dataframe(), ahrq_xwalk.dataframe(), \
        tribal_zips.dataframe(), loincs_to_remove.dataframe()

    # Solve death dupes - deaths have no primary key
    if domain.lower() == "death":
        p_key = "real_primary_key"
        foundry_df = foundry_df.withColumn(p_key, F.monotonically_increasing_id())
    else:
        p_key = DOMAIN_PKEYS[domain]

    bad_rows_dfs = []

    # --------------------ALL domains
    # -----CLEAN FREE TEXT
    # Clean free text columns to address PHI and site-identifying information.
    foundry_df, nulled_rows_df = clean_free_text_cols(foundry_df, domain, p_key, ctx)
    # Log the rows that were nulled if this domain has any free text columns/any rows were flagged
    if nulled_rows_df:
        nulled_rows.write_dataframe(nulled_rows_df)

    # --------------------DOMAINS with PERSON_ID
    # -----BAD PERSON_IDS
    if "person_id" in foundry_df.columns:
        # Remove person ids based on lds_clean_removed_person_ids dataset
        # Keep only one row per person_id
        removed_person_ids_distinct = removed_person_ids \
            .dropDuplicates(["person_id"]) \
            .where(F.col("person_id").isNotNull()) \
            .withColumn("col_name", F.lit("person_id")) \
            .withColumn("value", F.col("person_id")) \
            .select("col_name",
                    "value",
                    "removal_reason")

        column_values_dict = get_bad_vals_dict(removed_person_ids_distinct)
        for (col_name, val_reason_dict) in column_values_dict.items():
            bad_person_id_rows = get_bad_rows_by_column_val(foundry_df,
                                                            col_name,
                                                            val_reason_dict)
            if bad_person_id_rows:
                bad_rows_dfs.append(bad_person_id_rows)

        # Remove rows with null person_ids
        null_person_id_rows = foundry_df \
            .where(F.col("person_id").isNull()) \
            .withColumn("removal_reason", F.lit("NULL_PERSON_ID"))
        if null_person_id_rows:
            bad_rows_dfs.append(null_person_id_rows)

    # --------------------PERSON domain
    if domain.lower() == "person":
        # Null person_source_value column, which can contain site names
        foundry_df = clear_person_source_value(foundry_df)
        # Handle records with AN/AI race_concept_id (map to "Other" instead)
        foundry_df = filter_anai_records(foundry_df)

    # --------------------ANY domain -- removing rows based on LOINCS_TO_REMOVE input df
    # Filter to current domain and update col_name based on current domain
    # ie: '_concept_id' --> 'measurement_concept_id'
    bad_loincs = loincs_to_remove
    bad_loincs = bad_loincs.filter(F.array_contains(F.col("domains"), domain.lower()))
    bad_loincs = bad_loincs \
        .select('col_name', 'value', 'removal_reason') \
        .withColumn('col_name', F.concat(F.lit(domain.lower()), F.col('col_name')))

    if bad_loincs:
        # Grouping by 'column', create a dictionary of values:removal_reasons
        bad_loincs = bad_loincs \
            .groupBy('col_name').agg(
                F.map_from_entries(
                    F.collect_list(
                        F.struct('value', 'removal_reason'))).alias('val_reason_dict'))
        # Create a dictionary of form {col_name : {value : removal_reason, ...}, ...}
        column_values_dict = bad_loincs.rdd.map(lambda x: (x[0], x[1])).collectAsMap()  # noqa

        # Get bad rows by searching each col_name for values
        for (col_name, val_reason_dict) in column_values_dict.items():
            bad_loinc_rows = get_bad_rows_by_column_val(foundry_df,
                                                        col_name,
                                                        val_reason_dict)
            if bad_loinc_rows:
                bad_rows_dfs.append(bad_loinc_rows)

    # --------------------CONDITION_OCCURRENCE or OBSERVATION domains
    if domain.lower() in ["condition_occurrence", "observation"]:
        # Remove records containing AHRQ codes
        concept_col_dict = {
            "condition_occurrence": "condition_concept_id",
            "observation": "observation_concept_id"
        }
        source_col_dict = {
            "condition_occurrence": "condition_source_value",
            "observation": "observation_source_value"
        }

        ahrq_concept_ids = ahrq_xwalk.select("standard_concept_id").distinct()
        ahrq_source_values = ahrq_xwalk.select("icd10_source_code").distinct()

        # -----AHRQ_CONCEPT_ID
        # create dictionary of form {value : removal_reason}
        ahrq_concept_id_dict = ahrq_concept_ids.rdd.map(lambda x: (x[0], 'AHRQ_CONCEPT_ID')).collectAsMap()  # noqa
        ahrq_concept_id_rows = \
            get_bad_rows_by_column_val(foundry_df,
                                        concept_col_dict[domain.lower()],
                                        ahrq_concept_id_dict)
        if ahrq_concept_id_rows:
            bad_rows_dfs.append(ahrq_concept_id_rows)

        # -----AHRQ_SOURCE_VALUE
        # create dictionary of form {value : removal_reason}
        ahrq_source_values_dict = ahrq_source_values.rdd.map(lambda x: (x[0], 'AHRQ_SOURCE_VALUE')).collectAsMap()  # noqa
        ahrq_source_value_rows = \
            get_bad_rows_by_column_val(foundry_df,
                                        source_col_dict[domain.lower()],
                                        ahrq_source_values_dict)
        if ahrq_source_value_rows:
            bad_rows_dfs.append(ahrq_source_value_rows)

    # --------------------CARE_SITE domain
    if domain.lower() == "care_site":
        foundry_df = foundry_df.withColumn("care_site_name", F.lit(None).cast('string'))

    # --------------------LOCATION domain
    if domain.lower() == "location":
        # -----TRIBAL_ZIP_CODE
        tribal_zips = tribal_zips.select('Zip_Code')
        # create dictionary of form {value : removal_reason}
        tribal_zips_dict = tribal_zips.rdd.map(lambda x: (x[0], 'TRIBAL_ZIP_CODE')).collectAsMap()  # noqa
        tribal_zip_rows = \
            get_bad_rows_by_column_val(foundry_df,
                                        'zip',
                                        tribal_zips_dict)
        # add to list of bad_rows_dfs if not empty
        if tribal_zip_rows:
            bad_rows_dfs.append(tribal_zip_rows)

        # Clean up the many zip formats
        zipcol = F.trim(F.col("zip"))
        foundry_df = foundry_df.withColumn(
            "zip",

            # Standard 5 digit
            F.when(zipcol.rlike("^\\d{5}$"), zipcol)

            # 123XX format
            .when(zipcol.rlike("^\\d{3}XX$"), F.substring(zipcol, 1, 3))

            # Standard 3 digit
            .when(zipcol.rlike("^\\d{3}$"), zipcol)

            # 9 digit with a dash
            .when(zipcol.rlike("^\\d{5}-\\d{4}$"), F.substring(zipcol, 1, 5))

            # 9 digit with no dash
            .when(zipcol.rlike("^\\d{9}$"), F.substring(zipcol, 1, 5))

            # Null everything else. There are other formats, such as Canadian, the string "Unknown",
            # the string "OT", 5 digits then a dash then less than 4 digits,
            # and every number of digits from 2-8.
            .otherwise(F.lit(None).cast('string'))
        )

    # --------------------VISIT_DETAIL domain
    if domain.lower() == "visit_detail":
        # Null out the source value
        foundry_df = foundry_df.withColumn("visit_detail_source_value", F.lit(None).cast("string"))

    # --------------------NOTE_NLP domain
    if domain.lower() == "note_nlp":
        # Null out the time values - they are the time of the NLP processing, not the time of the EHR data
        foundry_df = foundry_df.withColumn("nlp_date", F.lit(None).cast("date"))
        foundry_df = foundry_df.withColumn("nlp_datetime", F.lit(None).cast("timestamp"))

        # Split
        foundry_df = foundry_df.withColumn("term_modifiers_split", F.split("term_modifiers", "[,;]"))

        foundry_df = foundry_df.withColumn("term_modifier_certainty", F.filter("term_modifiers_split", lambda col: col.startswith("certainty=")))
        foundry_df = foundry_df.withColumn("term_modifier_subject", F.filter("term_modifiers_split", lambda col: col.startswith("subject=") | col.startswith("experiencer=")))
        foundry_df = foundry_df.withColumn("term_modifier_status", F.filter("term_modifiers_split", lambda col: col.startswith("status=")))

        foundry_df = foundry_df.withColumn("term_modifier_certainty", F.split(F.col("term_modifier_certainty").getItem(0), "=").getItem(1))
        foundry_df = foundry_df.withColumn("term_modifier_subject", F.split(F.col("term_modifier_subject").getItem(0), "=").getItem(1))
        foundry_df = foundry_df.withColumn("term_modifier_status", F.split(F.col("term_modifier_status").getItem(0), "=").getItem(1))

        foundry_df = foundry_df.drop("term_modifiers_split")

    # --------------------DEVICE_EXPOSURE domain
    if domain.lower() == "device_exposure":
        foundry_df = foundry_df.withColumn("device_source_value", F.lit(None).cast("string"))

    # --------------------ALL domains
    # Remove bad rows
    if len(bad_rows_dfs) > 0:
        removed_rows_df = D.union_many(*bad_rows_dfs, spark_session=ctx.spark_session)
        # Remove dupes -- some rows may be caught by more than one check, but we only want to record it once
        removed_rows_df = removed_rows_df.dropDuplicates([p_key])
        foundry_df = foundry_df.join(removed_rows_df.select(p_key),
                                        on=p_key,
                                        how="left_anti")
        removed_rows.write_dataframe(removed_rows_df)

    else: 
        removed_rows.write_dataframe(ctx.spark_session.createDataFrame([], schema=foundry_df.schema))

    if domain.lower() == "death":
        foundry_df = foundry_df.drop(p_key)

    processed.write_dataframe(foundry_df)
