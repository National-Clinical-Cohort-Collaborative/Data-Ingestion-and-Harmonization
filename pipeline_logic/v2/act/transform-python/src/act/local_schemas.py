from pyspark.sql import types as T


complete_domain_schema_dict = {
    'concept_dimension': {
        'CONCEPT_PATH':  T.StringType(),
        'CONCEPT_CD':  T.StringType(),
        'NAME_CHAR':  T.StringType(),
        'UPDATE_DATE':  T.TimestampType(),
        'DOWNLOAD_DATE':  T.TimestampType(),
        'IMPORT_DATE':  T.TimestampType(),
        'SOURCESYSTEM_CD':  T.StringType(),
        'UPLOAD_ID':  T.IntegerType(),
    },

    "control_map": {
        "CASE_PATID": T.StringType(),
        "BUDDY_NUM": T.IntegerType(),
        "CONTROL_PATID": T.StringType(),
        "CASE_AGE": T.IntegerType(),
        "CASE_SEX": T.StringType(),
        "CASE_RACE": T.StringType(),
        "CASE_ETHN": T.StringType(),
        "CONTROL_AGE": T.IntegerType(),
        "CONTROL_SEX": T.StringType(),
        "CONTROL_RACE": T.StringType(),
        "CONTROL_ETHN": T.StringType()
    },

    "note": {
        "NOTE_ID": T.StringType(),
        "PERSON_ID": T.LongType(),
        "NOTE_DATE": T.DateType(),
        "NOTE_DATETIME": T.TimestampType(),
        "NOTE_TYPE_CONCEPT_ID": T.IntegerType(),
        "NOTE_CLASS_CONCEPT_ID": T.IntegerType(),
        "NOTE_TITLE": T.StringType(),
        "NOTE_TEXT": T.StringType(),
        "ENCODING_CONCEPT_ID": T.IntegerType(),
        "LANGUAGE_CONCEPT_ID": T.IntegerType(),
        "PROVIDER_ID": T.IntegerType(),
        "VISIT_OCCURRENCE_ID": T.IntegerType(),
        "VISIT_DETAIL_ID": T.IntegerType(),
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
        "TERM_MODIFIERS": T.StringType()
    },

    'observation_fact': {
        'ENCOUNTER_NUM':  T.StringType(),
        'CONCEPT_CD':  T.StringType(),
        'PROVIDER_ID':  T.StringType(),
        'START_DATE':  T.TimestampType(),
        'PATIENT_NUM':  T.StringType(),
        'MODIFIER_CD':  T.StringType(),
        'INSTANCE_NUM':  T.StringType(),
        'VALTYPE_CD':  T.StringType(),
        'TVAL_CHAR':  T.StringType(),
        'NVAL_NUM':  T.DecimalType(18, 5),
        'VALUEFLAG_CD':  T.StringType(),
        'QUANTITY_NUM':  T.DecimalType(18, 5),
        'UNITS_CD':  T.StringType(),
        'END_DATE':  T.TimestampType(),
        'LOCATION_CD':  T.StringType(),
        'OBSERVATION_BLOB':  T.StringType(),
        'CONFIDENCE_NUM':  T.DecimalType(18, 5),
        'UPDATE_DATE':  T.TimestampType(),
        'DOWNLOAD_DATE':  T.TimestampType(),
        'IMPORT_DATE':  T.TimestampType(),
        'SOURCESYSTEM_CD':  T.StringType(),
        'UPLOAD_ID':  T.IntegerType(),
    },

    'patient_dimension': {
        'PATIENT_NUM':  T.StringType(),
        'VITAL_STATUS_CD':  T.StringType(),
        'BIRTH_DATE':  T.TimestampType(),
        'DEATH_DATE':  T.TimestampType(),
        'SEX_CD':  T.StringType(),
        'AGE_IN_YEARS_NUM':  T.IntegerType(),
        'LANGUAGE_CD':  T.StringType(),
        'RACE_CD':  T.StringType(),
        'MARITAL_STATUS_CD':  T.StringType(),
        'RELIGION_CD':  T.StringType(),
        'ZIP_CD':  T.StringType(),
        'STATECITYZIP_PATH':  T.StringType(),
        'PATIENT_BLOB':  T.StringType(),
        'UPDATE_DATE':  T.TimestampType(),
        'DOWNLOAD_DATE':  T.TimestampType(),
        'IMPORT_DATE':  T.TimestampType(),
        'SOURCESYSTEM_CD':  T.StringType(),
        'UPLOAD_ID':  T.IntegerType(),
        'INCOME_CD': T.StringType(),
        "ETHNICITY_CD": T.StringType()
    },

    'visit_dimension': {
        'ENCOUNTER_NUM':  T.StringType(),
        'PATIENT_NUM':  T.StringType(),
        'ACTIVE_STATUS_CD':  T.StringType(),
        'START_DATE':  T.TimestampType(),
        'END_DATE':  T.TimestampType(),
        'INOUT_CD':  T.StringType(),
        'LOCATION_CD':  T.StringType(),
        'LOCATION_PATH': T.StringType(),
        'LENGTH_OF_STAY': T.StringType(),
        'VISIT_BLOB':  T.StringType(),
        'UPDATE_DATE':  T.TimestampType(),
        'DOWNLOAD_DATE':  T.TimestampType(),
        'IMPORT_DATE':  T.TimestampType(),
        'SOURCESYSTEM_CD':  T.StringType(),
        'UPLOAD_ID':  T.IntegerType(),
    },
}

required_domain_schema_dict = {
    'concept_dimension': {
        'CONCEPT_PATH':  T.StringType(),
        'CONCEPT_CD':  T.StringType(),
        'NAME_CHAR':  T.StringType(),
    },

    'control_map': {
        "CASE_PATID": T.StringType(),
        "BUDDY_NUM": T.IntegerType(),
        "CONTROL_PATID": T.StringType()
    },

    'note': {},

    'note_nlp': {},

    'observation_fact': {
        'ENCOUNTER_NUM':  T.StringType(),
        'CONCEPT_CD':  T.StringType(),
        'PROVIDER_ID':  T.StringType(),
        'START_DATE':  T.TimestampType(),
        'PATIENT_NUM':  T.StringType(),
        'MODIFIER_CD':  T.StringType(),
        'INSTANCE_NUM':  T.StringType(),
    },

    'patient_dimension': {
        'PATIENT_NUM':  T.StringType(),
    },

    'visit_dimension': {
        'ENCOUNTER_NUM':  T.StringType(),
        'PATIENT_NUM':  T.StringType(),
    },
}

act_local_code_map_schema = {
    "ACT_STANDARD_CODE": T.StringType(),
    "LOCAL_CONCEPT_CD": T.StringType(),
    "NAME_CHAR": T.StringType(),
    "PARENT_CONCEPT_PATH": T.StringType(),
    "CONCEPT_PATH": T.StringType(),
    "PATH_ELEMENT": T.StringType()
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

n3c_vocab_map_schema = {
    "LOCAL_PREFIX": T.StringType(),
    "OMOP_VOCAB": T.StringType()
}

metadata_schemas = {
    "act_standard2local_code_map": act_local_code_map_schema,
    "data_counts": data_counts_schema,
    "manifest": manifest_schema,
    "n3c_vocab_map": n3c_vocab_map_schema
}
