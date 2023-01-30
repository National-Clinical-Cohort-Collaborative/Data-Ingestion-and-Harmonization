from pyspark.sql import types as T


complete_domain_schema_dict = {
    "adt": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "ADT_EVENT_START_DATETIME": T.TimestampType(),
        "ADT_EVENT_END_DATETIME": T.TimestampType(),
        "ADT_EVENT_LOCATION": T.StringType(),
        "ADT_EVENT_SERVICE": T.StringType(),
        "ADT_EVENT_ACCOMMODATION_CODE": T.StringType(),
        "ADT_EVENT_LEVEL_OF_CARE": T.StringType(),
        "ADT_EVENT_IS_ICU": T.StringType(),
        "ADT_EVENT_IS_ED": T.StringType()
    },

    "control_map": {
        "CASE_PATIENT_ID": T.StringType(),
        "BUDDY_NUM": T.IntegerType(),
        "CONTROL_PATIENT_ID": T.StringType(),
        "CASE_AGE": T.IntegerType(),
        "CASE_SEX": T.StringType(),
        "CASE_RACE": T.StringType(),
        "CASE_ETHNICITY": T.StringType(),
        "CONTROL_AGE": T.IntegerType(),
        "CONTROL_SEX": T.StringType(),
        "CONTROL_RACE": T.StringType(),
        "CONTROL_ETHNICITY": T.StringType()
    },

    "diagnosis": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "DX_CODE_SYSTEM": T.StringType(),
        "DX_CODE": T.StringType(),
        "DATE": T.StringType(),
        "DX_DESCRIPTION": T.StringType(),
        "PRINCIPAL_INDICATOR": T.StringType(),
        "DX_SOURCE": T.StringType(),
        "ORPHAN_FLAG": T.StringType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType(),
    },

    "encounter": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "ENCOUNTER_TYPE": T.StringType(),
        "START_DATE": T.DateType(),
        "END_DATE": T.DateType(),
        "LENGTH_OF_STAY": T.IntegerType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "MAPPED_ENCOUNTER_TYPE": T.StringType(),
        "IS_LONG_COVID": T.BooleanType()
    },

    "lab_result": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "LAB_CODE_SYSTEM": T.StringType(),
        "LAB_CODE": T.StringType(),
        "LAB_DESCRIPTION": T.StringType(),
        "BATTERY_CODE_SYSTEM": T.StringType(),
        "BATTERY_CODE": T.StringType(),
        "BATTERY_DESC": T.StringType(),
        "SECTION": T.StringType(),
        "NORMAL_RANGE": T.StringType(),
        "TEST_DATE": T.DateType(),
        "RESULT_TYPE": T.StringType(),
        "NUMERIC_RESULT_VAL": T.DoubleType(),
        "TEXT_RESULT_VAL": T.StringType(),
        "UNITS_OF_MEASURE": T.StringType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType(),
        "MAPPED_TEXT_RESULT_VAL": T.StringType(),
    },

    "medication": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "RX_CODE_SYSTEM": T.StringType(),
        "RX_CODE": T.StringType(),
        "RX_DESCRIPTION": T.StringType(),
        "ALT_DRUG_CODE_SYS": T.StringType(),
        "ALT_DRUG_CODE": T.StringType(),
        "START_DATE": T.DateType(),
        "ROUTE_OF_ADMINISTRATION": T.StringType(),
        "UNITS_PER_ADMINISTRATION": T.DoubleType(),
        "FREQUENCY": T.StringType(),
        "STRENGTH": T.StringType(),
        "FORM": T.StringType(),
        "DURATION": T.DoubleType(),
        "REFILLS": T.DoubleType(),
        "RX_SOURCE": T.StringType(),
        "INDICATION_CODE_SYSTEM": T.StringType(),
        "INDICATION_CODE": T.StringType(),
        "INDICATION_DESC": T.StringType(),
        "ALT_DRUG_NAME": T.StringType(),
        "CLINICAL_DRUG": T.StringType(),
        "END_DATE": T.TimestampType(),
        "QTY_DISPENSED": T.DoubleType(),
        "DOSE_AMOUNT": T.DoubleType(),
        "DOSE_UNIT": T.StringType(),
        "BRAND": T.StringType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType()
    },

    "note": {
        "NOTE_ID": T.StringType(),
        "PERSON_ID": T.StringType(),
        "NOTE_DATE": T.DateType(),
        "NOTE_DATETIME": T.TimestampType(),
        "NOTE_TYPE_CONCEPT_ID": T.IntegerType(),
        "NOTE_CLASS_CONCEPT_ID": T.IntegerType(),
        "NOTE_TITLE": T.StringType(),
        "NOTE_TEXT": T.StringType(),
        "ENCODING_CONCEPT_ID": T.IntegerType(),
        "LANGUAGE_CONCEPT_ID": T.IntegerType(),
        "PROVIDER_ID": T.IntegerType(),
        "VISIT_OCCURRENCE_ID": T.LongType(),
        "VISIT_DETAIL_ID": T.LongType(),
        "NOTE_SOURCE_VALUE": T.StringType(),
    },

    "note_nlp": {
        "NOTE_NLP_ID": T.StringType(),
        "NOTE_ID": T.StringType(),
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

    "patient": {
        "PATIENT_ID": T.StringType(),
        "BIRTH_DATE": T.StringType(),
        "VITAL_STATUS": T.StringType(),
        "DEATH_DATE": T.DateType(),
        "POSTAL_CODE": T.StringType(),
        "SEX": T.StringType(),
        "RACE": T.StringType(),
        "ETHNICITY": T.StringType(),
        "LANGUAGE": T.StringType(),
        "MARITAL_STATUS": T.StringType(),
        "SMOKING_STATUS": T.StringType(),
        "MAPPED_SEX": T.StringType(),
        "MAPPED_RACE": T.StringType(),
        "MAPPED_ETHNICITY": T.StringType(),
        "MAPPED_MARITAL_STATUS": T.StringType()
    },

    "procedure": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "PX_CODE_SYSTEM": T.StringType(),
        "PX_CODE": T.StringType(),
        "PX_DESCRIPTION": T.StringType(),
        "DATE": T.DateType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType()
    },

    "vital_signs": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "MEASURE_DATE": T.DateType(),
        "VITAL_CODE_SYSTEM": T.StringType(),
        "VITAL_CODE": T.StringType(),
        "VITAL_DESCRIPTION": T.StringType(),
        "UNIT_OF_MEASURE": T.StringType(),
        "RESULT_TYPE": T.StringType(),
        "NUMERIC_RESULT_VAL": T.DoubleType(),
        "TEXT_RESULT_VAL": T.StringType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType(),
        "MAPPED_TEXT_RESULT_VAL": T.StringType(),
    }
}

required_domain_schema_dict = {
    "adt": {},

    "control_map": {
        "CASE_PATIENT_ID": T.StringType(),
        "BUDDY_NUM": T.IntegerType(),
        "CONTROL_PATIENT_ID": T.StringType()
    },

    "diagnosis": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "DX_CODE_SYSTEM": T.StringType(),
        "DX_CODE": T.StringType(),
        "DATE": T.StringType(),
        "DX_DESCRIPTION": T.StringType(),
        "PRINCIPAL_INDICATOR": T.StringType(),
        "DX_SOURCE": T.StringType(),
        "ORPHAN_FLAG": T.StringType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType(),
    },

    "encounter": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "ENCOUNTER_TYPE": T.StringType(),
        "START_DATE": T.DateType(),
        "END_DATE": T.DateType(),
        "LENGTH_OF_STAY": T.IntegerType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "MAPPED_ENCOUNTER_TYPE": T.StringType()
    },

    "lab_result": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "LAB_CODE_SYSTEM": T.StringType(),
        "LAB_CODE": T.StringType(),
        "LAB_DESCRIPTION": T.StringType(),
        "BATTERY_CODE_SYSTEM": T.StringType(),
        "BATTERY_CODE": T.StringType(),
        "BATTERY_DESC": T.StringType(),
        "SECTION": T.StringType(),
        "NORMAL_RANGE": T.StringType(),
        "TEST_DATE": T.DateType(),
        "RESULT_TYPE": T.StringType(),
        "NUMERIC_RESULT_VAL": T.DoubleType(),
        "TEXT_RESULT_VAL": T.StringType(),
        "UNITS_OF_MEASURE": T.StringType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType(),
        "MAPPED_TEXT_RESULT_VAL": T.StringType(),
    },

    "medication": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "RX_CODE_SYSTEM": T.StringType(),
        "RX_CODE": T.StringType(),
        "RX_DESCRIPTION": T.StringType(),
        "ALT_DRUG_CODE_SYS": T.StringType(),
        "ALT_DRUG_CODE": T.StringType(),
        "START_DATE": T.DateType(),
        "ROUTE_OF_ADMINISTRATION": T.StringType(),
        "UNITS_PER_ADMINISTRATION": T.DoubleType(),
        "FREQUENCY": T.StringType(),
        "STRENGTH": T.StringType(),
        "FORM": T.StringType(),
        "DURATION": T.DoubleType(),
        "REFILLS": T.DoubleType(),
        "RX_SOURCE": T.StringType(),
        "INDICATION_CODE_SYSTEM": T.StringType(),
        "INDICATION_CODE": T.StringType(),
        "INDICATION_DESC": T.StringType(),
        "ALT_DRUG_NAME": T.StringType(),
        "CLINICAL_DRUG": T.StringType(),
        "END_DATE": T.TimestampType(),
        "QTY_DISPENSED": T.DoubleType(),
        "DOSE_AMOUNT": T.DoubleType(),
        "DOSE_UNIT": T.StringType(),
        "BRAND": T.StringType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType()
    },

    "note": {
    },

    "note_nlp": {
    },

    "patient": {
        "PATIENT_ID": T.StringType(),
        "BIRTH_DATE": T.StringType(),
        "VITAL_STATUS": T.StringType(),
        "DEATH_DATE": T.DateType(),
        "POSTAL_CODE": T.StringType(),
        "SEX": T.StringType(),
        "RACE": T.StringType(),
        "ETHNICITY": T.StringType(),
        "LANGUAGE": T.StringType(),
        "MARITAL_STATUS": T.StringType(),
        "SMOKING_STATUS": T.StringType(),
        "MAPPED_SEX": T.StringType(),
        "MAPPED_RACE": T.StringType(),
        "MAPPED_ETHNICITY": T.StringType(),
        "MAPPED_MARITAL_STATUS": T.StringType()
    },

    "procedure": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "PX_CODE_SYSTEM": T.StringType(),
        "PX_CODE": T.StringType(),
        "PX_DESCRIPTION": T.StringType(),
        "DATE": T.DateType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType()
    },

    "vital_signs": {
        "PATIENT_ID": T.StringType(),
        "ENCOUNTER_ID": T.StringType(),
        "MEASURE_DATE": T.DateType(),
        "VITAL_CODE_SYSTEM": T.StringType(),
        "VITAL_CODE": T.StringType(),
        "VITAL_DESCRIPTION": T.StringType(),
        "UNIT_OF_MEASURE": T.StringType(),
        "RESULT_TYPE": T.StringType(),
        "NUMERIC_RESULT_VAL": T.DoubleType(),
        "TEXT_RESULT_VAL": T.StringType(),
        "ORPHAN_FLAG": T.BooleanType(),
        "ORPHAN_REASON": T.StringType(),
        "MAPPED_CODE_SYSTEM": T.StringType(),
        "MAPPED_CODE": T.StringType(),
        "MAPPED_TEXT_RESULT_VAL": T.StringType(),
    }
}

control_map_schema = {
    "BUDDY_NUM": T.IntegerType(),
    "CASE_PATIENT_ID": T.StringType(),
    "CONTROL_PATIENT_ID": T.StringType(),
    "CASE_AGE": T.IntegerType(),
    "CASE_SEX": T.StringType(),
    "CASE_RACE": T.StringType(),
    "CASE_ETHNICITY": T.StringType(),
    "CONTROL_AGE": T.IntegerType(),
    "CONTROL_SEX": T.StringType(),
    "CONTROL_RACE": T.StringType(),
    "CONTROL_ETHNICITY": T.StringType()
}

data_counts_schema = {
    "TABLE_NAME": T.StringType(),
    "ROW_COUNT": T.StringType()
}

manifest_schema = {
    "SITE_ABBREV": T.StringType(),
    "SITE_NAME": T.StringType(),
    "CONTACT_NAME": T.StringType(),
    "CONTACT_EMAIL": T.StringType(),
    "CDM_NAME": T.StringType(),
    "CDM_VERSION": T.StringType(),
    "VOCABULARY_VERSION": T.StringType(),
    "N3C_PHENOTYPE_YN": T.StringType(),
    "N3C_PHENOTYPE_VERSION": T.StringType(),
    "SHIFT_DATE_YN": T.StringType(),
    "MAX_NUM_SHIFT_DAYS": T.StringType(),
    "RUN_DATE": T.StringType(),
    "UPDATE_DATE": T.StringType(),
    "NEXT_SUBMISSION_DATE": T.StringType(),
}

metadata_schemas = {
    "control_map": control_map_schema,
    "data_counts": data_counts_schema,
    "manifest": manifest_schema
}
