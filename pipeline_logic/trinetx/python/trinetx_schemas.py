from collections import OrderedDict
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField


def schema_dict_to_struct(schema_dict, all_string_type):
    field_list = []
    for col_name, col_type in schema_dict.items():
        if all_string_type:
            field_list.append(StructField(col_name, T.StringType(), True))
        else:
            field_list.append(StructField(col_name, col_type, True))

    struct_schema = StructType(field_list)
    return struct_schema


def schema_dict_all_string_type(schema_dict, all_lowercase=False):
    result = OrderedDict()
    for col_name in schema_dict.keys():
        if all_lowercase:
            col_name = col_name.lower()
        result[col_name] = T.StringType()
    return result


complete_domain_schema_dict_string_type = {
    'diagnosis': OrderedDict([
        ('PATIENT_ID', T.StringType()),
        ('ENCOUNTER_ID', T.StringType()),
        ('DX_CODE_SYSTEM', T.StringType()),
        ('DX_CODE', T.StringType()),
        ('DATE', T.StringType()),
        ('DX_DESCRIPTION', T.StringType()),
        ('PRINCIPAL_INDICATOR', T.StringType()),
        ('DX_SOURCE', T.StringType()),
        ('ORPHAN_FLAG', T.StringType()),
        ('ORPHAN_REASON', T.StringType()),
        ('MAPPED_CODE_SYSTEM', T.StringType()),
        ('MAPPED_CODE', T.StringType()),
    ]),

    'encounter': OrderedDict([
        ('PATIENT_ID', T.StringType()),
        ('ENCOUNTER_ID', T.StringType()),
        ('ENCOUNTER_TYPE', T.StringType()),
        ('START_DATE', T.StringType()),
        ('END_DATE', T.StringType()),
        ('LENGTH_OF_STAY', T.StringType()),
        ('ORPHAN_FLAG', T.StringType()),
        ('MAPPED_ENCOUNTER_TYPE', T.StringType())
    ]),

    "lab_result": OrderedDict([
        ('PATIENT_ID', T.StringType()),
        ('ENCOUNTER_ID', T.StringType()),
        ('LAB_CODE_SYSTEM', T.StringType()),
        ('LAB_CODE', T.StringType()),
        ('LAB_DESCRIPTION', T.StringType()),
        ('BATTERY_CODE_SYSTEM', T.StringType()),
        ('BATTERY_CODE', T.StringType()),
        ('BATTERY_DESC', T.StringType()),
        ('SECTION', T.StringType()),
        ('NORMAL_RANGE', T.StringType()),
        ('TEST_DATE', T.StringType()),
        ('RESULT_TYPE', T.StringType()),
        ('NUMERIC_RESULT_VAL', T.StringType()),
        ('TEXT_RESULT_VAL', T.StringType()),
        ('UNITS_OF_MEASURE', T.StringType()),
        ('ORPHAN_FLAG', T.StringType()),
        ('ORPHAN_REASON', T.StringType()),
        ('MAPPED_CODE_SYSTEM', T.StringType()),
        ('MAPPED_CODE', T.StringType()),
        ('MAPPED_TEXT_RESULT_VAL', T.StringType()),
    ]),

    "medication": OrderedDict([
        ('PATIENT_ID', T.StringType()),
        ('ENCOUNTER_ID', T.StringType()),
        ('RX_CODE_SYSTEM', T.StringType()),
        ('RX_CODE', T.StringType()),
        ('RX_DESCRIPTION', T.StringType()),
        ('ALT_DRUG_CODE_SYS', T.StringType()),
        ('ALT_DRUG_CODE', T.StringType()),
        ('START_DATE', T.StringType()),
        ('ROUTE_OF_ADMINISTRATION', T.StringType()),
        ('UNITS_PER_ADMINISTRATION', T.StringType()),
        ('FREQUENCY', T.StringType()),
        ('STRENGTH', T.StringType()),
        ('FORM', T.StringType()),
        ('DURATION', T.StringType()),
        ('REFILLS', T.StringType()),
        ('RX_SOURCE', T.StringType()),
        ('INDICATION_CODE_SYSTEM', T.StringType()),
        ('INDICATION_CODE', T.StringType()),
        ('INDICATION_DESC', T.StringType()),
        ('ALT_DRUG_NAME', T.StringType()),
        ('CLINICAL_DRUG', T.StringType()),
        ('END_DATE', T.StringType()),
        ('QTY_DISPENSED', T.StringType()),
        ('DOSE_AMOUNT', T.StringType()),
        ('DOSE_UNIT', T.StringType()),
        ('BRAND', T.StringType()),
        ('ORPHAN_FLAG', T.StringType()),
        ('ORPHAN_REASON', T.StringType()),
        ('MAPPED_CODE_SYSTEM', T.StringType()),
        ('MAPPED_CODE', T.StringType())
    ]),

    "patient": OrderedDict([
        ('PATIENT_ID', T.StringType()),
        ('BIRTH_DATE', T.StringType()),
        ('VITAL_STATUS', T.StringType()),
        ('DEATH_DATE', T.StringType()),
        ('POSTAL_CODE', T.StringType()),
        ('SEX', T.StringType()),
        ('RACE', T.StringType()),
        ('ETHNICITY', T.StringType()),
        ('LANGUAGE', T.StringType()),
        ('MARITAL_STATUS', T.StringType()),
        ('SMOKING_STATUS', T.StringType()),
        ('MAPPED_SEX', T.StringType()),
        ('MAPPED_RACE', T.StringType()),
        ('MAPPED_ETHNICITY', T.StringType()),
        ('MAPPED_MARITAL_STATUS', T.StringType())
    ]),

    "procedure": OrderedDict([
        ('PATIENT_ID', T.StringType()),
        ('ENCOUNTER_ID', T.StringType()),
        ('PX_CODE_SYSTEM', T.StringType()),
        ('PX_CODE', T.StringType()),
        ('PX_DESCRIPTION', T.StringType()),
        ('DATE', T.StringType()),
        ('ORPHAN_FLAG', T.StringType()),
        ('ORPHAN_REASON', T.StringType()),
        ('MAPPED_CODE_SYSTEM', T.StringType()),
        ('MAPPED_CODE', T.StringType())
    ]),

    "vital_signs": OrderedDict([
        ('PATIENT_ID', T.StringType()),
        ('ENCOUNTER_ID', T.StringType()),
        ('MEASURE_DATE', T.StringType()),
        ('VITAL_CODE_SYSTEM', T.StringType()),
        ('VITAL_CODE', T.StringType()),
        ('VITAL_DESCRIPTION', T.StringType()),
        ('UNIT_OF_MEASURE', T.StringType()),
        ('RESULT_TYPE', T.StringType()),
        ('NUMERIC_RESULT_VAL', T.StringType()),
        ('TEXT_RESULT_VAL', T.StringType()),
        ('ORPHAN_FLAG', T.StringType()),
        ('ORPHAN_REASON', T.StringType()),
        ('MAPPED_CODE_SYSTEM', T.StringType()),
        ('MAPPED_CODE', T.StringType()),
        ('MAPPED_TEXT_RESULT_VAL', T.StringType()),
    ]),
}
