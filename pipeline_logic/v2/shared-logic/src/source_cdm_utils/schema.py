from pyspark.sql import functions as F, types as T


# Used in step 2 of all CDMs
def apply_schema(df, schema_dict, include_payload=True):
    # Convert empty strings to null values
    exprs = [F.when(F.col(x) != "", F.col(x)).otherwise(None).alias(x) for x in df.columns]
    df = df.select(*exprs)

    # Make sure all column names are uppercase
    for original_col_name in df.columns:
        df = df.withColumnRenamed(original_col_name, original_col_name.upper())
    input_cols = df.columns

    # Iterate through all columns in OMOP domain schema
    # Cast those that are present to proper types, or create empty columns
    for col_name, col_type in schema_dict.items():
        if col_name in input_cols:
            if col_type == T.DateType() or col_type == T.TimestampType():
                # Handle dates/datetimes in Unix timestamp format or in regular string timestamp format
                df = df.withColumn(col_name, F.coalesce(F.from_unixtime(df[col_name]), df[col_name]))

                # Handle Oracle format (site 117)
                df = df.withColumn(
                    col_name,
                    F.when(
                        df[col_name].rlike("^\\d{2}-[A-Za-z]{3}-\\d{2} \\d{2}\\.\\d{2}\\.\\d{2}\\.\\d{3,9} (AM|PM)$"),
                        F.from_unixtime(F.unix_timestamp(df[col_name], format='dd-MMM-yy hh.mm.ss.SSS a'))
                    ).otherwise(df[col_name])
                )

                # Handle site 84's format
                df = df.withColumn(
                    col_name,
                    F.when(
                        df[col_name].rlike("^\\d{1,2}\\/\\d{1,2}\\/\\d{4} \\d{1,2}:\\d{2}:\\d{2} (AM|PM)$"),
                        F.from_unixtime(F.unix_timestamp(df[col_name], format='MM/dd/yy hh:mm:ss a'))
                    ).otherwise(df[col_name])
                )

            df = df.withColumn(col_name, df[col_name].cast(col_type))

        else:
            # Populate column with null if it isn't in input table
            df = df.withColumn(col_name, F.lit(None).cast(col_type))

    # Reorder columns according to schema. Make sure we pass-through the payload name column
    col_list = list(schema_dict.keys())
    if include_payload:
        col_list.append("PAYLOAD")
    df = df.select(col_list)

    # Rename columns to lowercase
    for original_col_name in df.columns:
        df = df.withColumnRenamed(original_col_name, original_col_name.lower())

    return df


# Create column to store an id for each site, to be used in SQL code for primary key generation
def add_site_id_col(df, site_id_df):
    site_id_df = site_id_df.dataframe()
    site_id = site_id_df.head().data_partner_id  # Noqa

    df = df.withColumn("data_partner_id", F.lit(site_id))
    df = df.withColumn("data_partner_id", df["data_partner_id"].cast(T.IntegerType()))
    return df


# Helper functions

def schema_dict_to_struct(schema_dict, all_string_type):
    field_list = []
    for col_name, col_type in schema_dict.items():
        if all_string_type:
            field_list.append(T.StructField(col_name, T.StringType()))
        else:
            field_list.append(T.StructField(col_name, col_type))

    struct_schema = T.StructType(field_list)
    return struct_schema


def schema_dict_all_string_type(schema_dict, all_lowercase=False, add_payload=False):
    result = {}

    for col_name in schema_dict.keys():
        if all_lowercase:
            col_name = col_name.lower()
        result[col_name] = T.StringType()

    if add_payload:
        result["payload"] = T.StringType()

    return result


omop_complete = {
    "care_site": {
        "CARE_SITE_ID": T.LongType(),
        "CARE_SITE_NAME": T.StringType(),
        "PLACE_OF_SERVICE_CONCEPT_ID": T.IntegerType(),
        "LOCATION_ID": T.LongType(),
        "CARE_SITE_SOURCE_VALUE": T.StringType(),
        "PLACE_OF_SERVICE_SOURCE_VALUE": T.StringType(),
    },
    "condition_era": {
        "CONDITION_ERA_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "CONDITION_CONCEPT_ID": T.IntegerType(),
        "CONDITION_ERA_START_DATE": T.DateType(),
        "CONDITION_ERA_END_DATE": T.DateType(),
        "CONDITION_OCCURRENCE_COUNT": T.IntegerType(),
    },
    "condition_occurrence": {
        "CONDITION_OCCURRENCE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "CONDITION_CONCEPT_ID": T.IntegerType(),
        "CONDITION_START_DATE": T.DateType(),
        "CONDITION_START_DATETIME": T.TimestampType(),
        "CONDITION_END_DATE": T.DateType(),
        "CONDITION_END_DATETIME": T.TimestampType(),
        "CONDITION_TYPE_CONCEPT_ID": T.IntegerType(),
        "STOP_REASON": T.StringType(),
        "PROVIDER_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "CONDITION_SOURCE_VALUE": T.StringType(),
        "CONDITION_SOURCE_CONCEPT_ID": T.IntegerType(),
        "CONDITION_STATUS_SOURCE_VALUE": T.StringType(),
        "CONDITION_STATUS_CONCEPT_ID": T.IntegerType(),
    },
    "control_map": {
        "CONTROL_MAP_ID": T.LongType(),
        "CASE_PERSON_ID": T.LongType(),
        "BUDDY_NUM": T.IntegerType(),
        "CONTROL_PERSON_ID": T.LongType(),
        "CASE_AGE": T.IntegerType(),
        "CASE_SEX": T.StringType(),
        "CASE_RACE": T.StringType(),
        "CASE_ETHN": T.StringType(),
        "CONTROL_AGE": T.IntegerType(),
        "CONTROL_SEX": T.StringType(),
        "CONTROL_RACE": T.StringType(),
        "CONTROL_ETHN": T.StringType()
    },
    "death": {
        "PERSON_ID": T.LongType(),
        "DEATH_DATE": T.DateType(),
        "DEATH_DATETIME": T.TimestampType(),
        "DEATH_TYPE_CONCEPT_ID": T.IntegerType(),
        "CAUSE_CONCEPT_ID": T.IntegerType(),
        "CAUSE_SOURCE_VALUE": T.StringType(),
        "CAUSE_SOURCE_CONCEPT_ID": T.IntegerType(),
    },
    "device_exposure": {
        "DEVICE_EXPOSURE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DEVICE_CONCEPT_ID": T.IntegerType(),
        "DEVICE_EXPOSURE_START_DATE": T.DateType(),
        "DEVICE_EXPOSURE_START_DATETIME": T.TimestampType(),
        "DEVICE_EXPOSURE_END_DATE": T.DateType(),
        "DEVICE_EXPOSURE_END_DATETIME": T.TimestampType(),
        "DEVICE_TYPE_CONCEPT_ID": T.IntegerType(),
        "UNIQUE_DEVICE_ID": T.StringType(),
        "QUANTITY": T.IntegerType(),
        "PROVIDER_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "DEVICE_SOURCE_VALUE": T.StringType(),
        "DEVICE_SOURCE_CONCEPT_ID": T.IntegerType(),
    },
    "dose_era": {
        "DOSE_ERA_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DRUG_CONCEPT_ID": T.IntegerType(),
        "UNIT_CONCEPT_ID": T.IntegerType(),
        "DOSE_VALUE": T.FloatType(),
        "DOSE_ERA_START_DATE": T.DateType(),
        "DOSE_ERA_END_DATE": T.DateType(),
    },
    "drug_era": {
        "DRUG_ERA_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DRUG_CONCEPT_ID": T.IntegerType(),
        "DRUG_ERA_START_DATE": T.DateType(),
        "DRUG_ERA_END_DATE": T.DateType(),
        "DRUG_EXPOSURE_COUNT": T.IntegerType(),
        "GAP_DAYS": T.IntegerType(),
    },
    "drug_exposure": {
        "DRUG_EXPOSURE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DRUG_CONCEPT_ID": T.IntegerType(),
        "DRUG_EXPOSURE_START_DATE": T.DateType(),
        "DRUG_EXPOSURE_START_DATETIME": T.TimestampType(),
        "DRUG_EXPOSURE_END_DATE": T.DateType(),
        "DRUG_EXPOSURE_END_DATETIME": T.TimestampType(),
        "VERBATIM_END_DATE": T.DateType(),
        "DRUG_TYPE_CONCEPT_ID": T.IntegerType(),
        "STOP_REASON": T.StringType(),
        "REFILLS": T.IntegerType(),
        "QUANTITY": T.FloatType(),
        "DAYS_SUPPLY": T.IntegerType(),
        "SIG": T.StringType(),
        "ROUTE_CONCEPT_ID": T.IntegerType(),
        "LOT_NUMBER": T.StringType(),
        "PROVIDER_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "DRUG_SOURCE_VALUE": T.StringType(),
        "DRUG_SOURCE_CONCEPT_ID": T.IntegerType(),
        "ROUTE_SOURCE_VALUE": T.StringType(),
        "DOSE_UNIT_SOURCE_VALUE": T.StringType(),
    },
    "location": {
        "LOCATION_ID": T.LongType(),
        "ADDRESS_1": T.StringType(),
        "ADDRESS_2": T.StringType(),
        "CITY": T.StringType(),
        "STATE": T.StringType(),
        "ZIP": T.StringType(),
        "COUNTY": T.StringType(),
        "LOCATION_SOURCE_VALUE": T.StringType(),
    },
    "measurement": {
        "MEASUREMENT_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "MEASUREMENT_CONCEPT_ID": T.IntegerType(),
        "MEASUREMENT_DATE": T.DateType(),
        "MEASUREMENT_DATETIME": T.TimestampType(),
        "MEASUREMENT_TIME": T.StringType(),
        "MEASUREMENT_TYPE_CONCEPT_ID": T.IntegerType(),
        "OPERATOR_CONCEPT_ID": T.IntegerType(),
        "VALUE_AS_NUMBER": T.DoubleType(),
        "VALUE_AS_CONCEPT_ID": T.IntegerType(),
        "UNIT_CONCEPT_ID": T.IntegerType(),
        "RANGE_LOW": T.FloatType(),
        "RANGE_HIGH": T.FloatType(),
        "PROVIDER_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "MEASUREMENT_SOURCE_VALUE": T.StringType(),
        "MEASUREMENT_SOURCE_CONCEPT_ID": T.IntegerType(),
        "UNIT_SOURCE_VALUE": T.StringType(),
        "VALUE_SOURCE_VALUE": T.StringType(),
    },
    "note": {
        "NOTE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "NOTE_DATE": T.DateType(),
        "NOTE_DATETIME": T.TimestampType(),
        "NOTE_TYPE_CONCEPT_ID": T.IntegerType(),
        "NOTE_CLASS_CONCEPT_ID": T.IntegerType(),
        "NOTE_TITLE": T.StringType(),
        "NOTE_TEXT": T.StringType(),
        "ENCODING_CONCEPT_ID": T.IntegerType(),
        "LANGUAGE_CONCEPT_ID": T.IntegerType(),
        "PROVIDER_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "NOTE_SOURCE_VALUE": T.StringType(),
    },
    "note_nlp": {
        "NOTE_NLP_ID": T.LongType(),
        "NOTE_ID": T.LongType(),
        "SECTION_CONCEPT_ID": T.IntegerType(),
        "SNIPPET": T.StringType(),
        "OFFSET": T.StringType(),
        "LEXICAL_VARIANT": T.StringType(),
        "NOTE_NLP_CONCEPT_ID": T.IntegerType(),
        "NOTE_NLP_SOURCE_CONCEPT_ID": T.IntegerType(),
        "NLP_SYSTEM": T.StringType(),
        "NLP_DATE": T.DateType(),
        "NLP_DATETIME": T.TimestampType(),
        "TERM_EXISTS": T.BooleanType(),
        "TERM_TEMPORAL": T.StringType(),
        "TERM_MODIFIERS": T.StringType(),
    },
    "observation": {
        "OBSERVATION_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "OBSERVATION_CONCEPT_ID": T.IntegerType(),
        "OBSERVATION_DATE": T.DateType(),
        "OBSERVATION_DATETIME": T.TimestampType(),
        "OBSERVATION_TYPE_CONCEPT_ID": T.IntegerType(),
        "VALUE_AS_NUMBER": T.DoubleType(),
        "VALUE_AS_STRING": T.StringType(),
        "VALUE_AS_CONCEPT_ID": T.IntegerType(),
        "QUALIFIER_CONCEPT_ID": T.IntegerType(),
        "UNIT_CONCEPT_ID": T.IntegerType(),
        "PROVIDER_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "OBSERVATION_SOURCE_VALUE": T.StringType(),
        "OBSERVATION_SOURCE_CONCEPT_ID": T.IntegerType(),
        "UNIT_SOURCE_VALUE": T.StringType(),
        "QUALIFIER_SOURCE_VALUE": T.StringType(),
    },
    "observation_period": {
        "OBSERVATION_PERIOD_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "OBSERVATION_PERIOD_START_DATE": T.DateType(),
        "OBSERVATION_PERIOD_END_DATE": T.DateType(),
        "PERIOD_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    'payer_plan_period': {
        'PAYER_PLAN_PERIOD_ID': T.LongType(),
        'PERSON_ID': T.LongType(),
        'PAYER_PLAN_PERIOD_START_DATE': T.DateType(),
        'PAYER_PLAN_PERIOD_END_DATE': T.DateType(),
        'PAYER_CONCEPT_ID': T.IntegerType(),
        'PAYER_SOURCE_VALUE': T.StringType(),
        'PAYER_SOURCE_CONCEPT_ID': T.IntegerType(),
        'PLAN_CONCEPT_ID': T.IntegerType(),
        'PLAN_SOURCE_VALUE': T.StringType(),
        'PLAN_SOURCE_CONCEPT_ID': T.IntegerType(),
        'SPONSOR_CONCEPT_ID': T.IntegerType(),
        'SPONSOR_SOURCE_VALUE': T.StringType(),
        'SPONSOR_SOURCE_CONCEPT_ID': T.IntegerType(),
        'FAMILY_SOURCE_VALUE': T.StringType(),
        'STOP_REASON_CONCEPT_ID': T.IntegerType(),
        'STOP_REASON_SOURCE_VALUE': T.StringType(),
        'STOP_REASON_SOURCE_CONCEPT_ID': T.IntegerType(),
    },
    "person": {
        "PERSON_ID": T.LongType(),
        "GENDER_CONCEPT_ID": T.IntegerType(),
        "YEAR_OF_BIRTH": T.IntegerType(),
        "MONTH_OF_BIRTH": T.IntegerType(),
        "DAY_OF_BIRTH": T.IntegerType(),
        "BIRTH_DATETIME": T.TimestampType(),
        "RACE_CONCEPT_ID": T.IntegerType(),
        "ETHNICITY_CONCEPT_ID": T.IntegerType(),
        "LOCATION_ID": T.LongType(),
        "PROVIDER_ID": T.LongType(),
        "CARE_SITE_ID": T.LongType(),
        "PERSON_SOURCE_VALUE": T.StringType(),
        "GENDER_SOURCE_VALUE": T.StringType(),
        "GENDER_SOURCE_CONCEPT_ID": T.IntegerType(),
        "RACE_SOURCE_VALUE": T.StringType(),
        "RACE_SOURCE_CONCEPT_ID": T.IntegerType(),
        "ETHNICITY_SOURCE_VALUE": T.StringType(),
        "ETHNICITY_SOURCE_CONCEPT_ID": T.IntegerType(),
    },
    "procedure_occurrence": {
        "PROCEDURE_OCCURRENCE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "PROCEDURE_CONCEPT_ID": T.IntegerType(),
        "PROCEDURE_DATE": T.DateType(),
        "PROCEDURE_DATETIME": T.TimestampType(),
        "PROCEDURE_TYPE_CONCEPT_ID": T.IntegerType(),
        "MODIFIER_CONCEPT_ID": T.IntegerType(),
        "QUANTITY": T.IntegerType(),
        "PROVIDER_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "PROCEDURE_SOURCE_VALUE": T.StringType(),
        "PROCEDURE_SOURCE_CONCEPT_ID": T.IntegerType(),
        "MODIFIER_SOURCE_VALUE": T.StringType(),
    },
    "provider": {
        "PROVIDER_ID": T.LongType(),
        "PROVIDER_NAME": T.StringType(),
        "NPI": T.StringType(),
        "DEA": T.StringType(),
        "SPECIALTY_CONCEPT_ID": T.IntegerType(),
        "CARE_SITE_ID": T.LongType(),
        "YEAR_OF_BIRTH": T.IntegerType(),
        "GENDER_CONCEPT_ID": T.IntegerType(),
        "PROVIDER_SOURCE_VALUE": T.StringType(),
        "SPECIALTY_SOURCE_VALUE": T.StringType(),
        "SPECIALTY_SOURCE_CONCEPT_ID": T.IntegerType(),
        "GENDER_SOURCE_VALUE": T.StringType(),
        "GENDER_SOURCE_CONCEPT_ID": T.IntegerType(),
    },
    "visit_detail": {
        "VISIT_DETAIL_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "VISIT_DETAIL_CONCEPT_ID": T.IntegerType(),
        "VISIT_DETAIL_START_DATE": T.DateType(),
        "VISIT_DETAIL_START_DATETIME": T.TimestampType(),
        "VISIT_DETAIL_END_DATE": T.DateType(),
        "VISIT_DETAIL_END_DATETIME": T.TimestampType(),
        "VISIT_DETAIL_TYPE_CONCEPT_ID": T.IntegerType(),
        "PROVIDER_ID": T.LongType(),
        "CARE_SITE_ID": T.LongType(),
        "VISIT_DETAIL_SOURCE_VALUE": T.StringType(),
        "VISIT_DETAIL_SOURCE_CONCEPT_ID": T.IntegerType(),
        "ADMITTING_SOURCE_VALUE": T.StringType(),
        "ADMITTING_SOURCE_CONCEPT_ID": T.IntegerType(),
        "DISCHARGE_TO_SOURCE_VALUE": T.StringType(),
        "DISCHARGE_TO_CONCEPT_ID": T.IntegerType(),
        "PRECEDING_VISIT_DETAIL_ID": T.LongType(),
        "VISIT_DETAIL_PARENT_ID": T.LongType(),
        "VISIT_OCCURRENCE_ID": T.LongType()
    },
    "visit_occurrence": {
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "VISIT_CONCEPT_ID": T.IntegerType(),
        "VISIT_START_DATE": T.DateType(),
        "VISIT_START_DATETIME": T.TimestampType(),
        "VISIT_END_DATE": T.DateType(),
        "VISIT_END_DATETIME": T.TimestampType(),
        "VISIT_TYPE_CONCEPT_ID": T.IntegerType(),
        "PROVIDER_ID": T.LongType(),
        "CARE_SITE_ID": T.LongType(),
        "VISIT_SOURCE_VALUE": T.StringType(),
        "VISIT_SOURCE_CONCEPT_ID": T.IntegerType(),
        "ADMITTING_SOURCE_CONCEPT_ID": T.IntegerType(),
        "ADMITTING_SOURCE_VALUE": T.StringType(),
        "DISCHARGE_TO_CONCEPT_ID": T.IntegerType(),
        "DISCHARGE_TO_SOURCE_VALUE": T.StringType(),
        "PRECEDING_VISIT_OCCURRENCE_ID": T.LongType(),
    },
}


omop_required = {
    "care_site": {
        "CARE_SITE_ID": T.LongType(),
    },
    "condition_era": {
        "CONDITION_ERA_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "CONDITION_CONCEPT_ID": T.IntegerType(),
        "CONDITION_ERA_START_DATE": T.DateType(),
        "CONDITION_ERA_END_DATE": T.DateType(),
    },
    "condition_occurrence": {
        "CONDITION_OCCURRENCE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "CONDITION_CONCEPT_ID": T.IntegerType(),
        "CONDITION_START_DATE": T.DateType(),
        "CONDITION_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    "control_map": {
        "CONTROL_MAP_ID": T.LongType(),
        "CASE_PERSON_ID": T.LongType(),
        "BUDDY_NUM": T.IntegerType()
    },
    "death": {
        "PERSON_ID": T.LongType(),
        "DEATH_DATE": T.DateType(),
    },
    "device_exposure": {
        "DEVICE_EXPOSURE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DEVICE_CONCEPT_ID": T.IntegerType(),
        "DEVICE_EXPOSURE_START_DATE": T.DateType(),
        "DEVICE_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    "dose_era": {
        "DOSE_ERA_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DRUG_CONCEPT_ID": T.IntegerType(),
        "UNIT_CONCEPT_ID": T.IntegerType(),
        "DOSE_VALUE": T.FloatType(),
        "DOSE_ERA_START_DATE": T.DateType(),
        "DOSE_ERA_END_DATE": T.DateType(),
    },
    "drug_era": {
        "DRUG_ERA_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DRUG_CONCEPT_ID": T.IntegerType(),
        "DRUG_ERA_START_DATE": T.DateType(),
        "DRUG_ERA_END_DATE": T.DateType(),
    },
    "drug_exposure": {
        "DRUG_EXPOSURE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "DRUG_CONCEPT_ID": T.IntegerType(),
        "DRUG_EXPOSURE_START_DATE": T.DateType(),
        "DRUG_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    "location": {
        "LOCATION_ID": T.LongType(),
    },
    "measurement": {
        "MEASUREMENT_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "MEASUREMENT_CONCEPT_ID": T.IntegerType(),
        "MEASUREMENT_DATE": T.DateType(),
        "MEASUREMENT_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    "note": {
        # "NOTE_ID": T.LongType(),
        # "PERSON_ID": T.LongType(),
        # "NOTE_DATE": T.DateType(),
        # "NOTE_TYPE_CONCEPT_ID": T.IntegerType(),
        # "NOTE_CLASS_TYPE_ID": T.IntegerType(),
        # "NOTE_TEXT": T.StringType(),
        # "ENCODING_CONCEPT_ID": T.IntegerType(),
        # "LANGUAGE_CONCEPT_ID": T.IntegerType(),
    },
    "note_nlp": {
        # "NOTE_NLP_ID": T.LongType(),
        # "NOTE_ID": T.LongType(),
        # "LEXICAL_VARIANT": T.StringType(),
        # "NLP_DATE": T.DateType(),
    },
    "observation": {
        "OBSERVATION_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "OBSERVATION_CONCEPT_ID": T.IntegerType(),
        "OBSERVATION_DATE": T.DateType(),
        "OBSERVATION_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    "observation_period": {
        "OBSERVATION_PERIOD_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "OBSERVATION_PERIOD_START_DATE": T.DateType(),
        "PERIOD_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    'payer_plan_period': {
        'PAYER_PLAN_PERIOD_ID': T.LongType(),
        'PERSON_ID': T.LongType(),
        'PAYER_PLAN_PERIOD_START_DATE': T.DateType(),
        'PAYER_PLAN_PERIOD_END_DATE': T.DateType(),
        'PAYER_CONCEPT_ID': T.IntegerType(),
    },
    "person": {
        "PERSON_ID": T.LongType(),
        "GENDER_CONCEPT_ID": T.IntegerType(),
        "YEAR_OF_BIRTH": T.IntegerType(),
        "ETHNICITY_CONCEPT_ID": T.IntegerType(),
    },
    "procedure_occurrence": {
        "PROCEDURE_OCCURRENCE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "PROCEDURE_CONCEPT_ID": T.IntegerType(),
        "PROCEDURE_DATE": T.DateType(),
        "PROCEDURE_TYPE_CONCEPT_ID": T.IntegerType(),
    },
    "provider": {
        "PROVIDER_ID": T.LongType(),
    },
    "visit_detail": {
        "VISIT_DETAIL_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "VISIT_DETAIL_CONCEPT_ID": T.IntegerType(),
        "VISIT_DETAIL_START_DATE": T.DateType(),
        "VISIT_DETAIL_END_DATE": T.DateType(),
        "VISIT_DETAIL_TYPE_CONCEPT_ID": T.IntegerType(),
        "VISIT_OCCURRENCE_ID": T.LongType()
    },
    "visit_occurrence": {
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "PERSON_ID": T.LongType(),
        "VISIT_CONCEPT_ID": T.IntegerType(),
        "VISIT_START_DATE": T.DateType(),
        "VISIT_TYPE_CONCEPT_ID": T.IntegerType(),
    }
}
