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


def schema_dict_all_string_type(schema_dict, all_uppercase=False):
    result = OrderedDict()
    for col_name in schema_dict.keys():
        if all_uppercase:
            col_name = col_name.upper()
        result[col_name] = T.StringType()
    return result


complete_domain_schema_dict = {
    'concept_dimension': OrderedDict([
        ('concept_path', T.StringType()),
        ('concept_cd', T.StringType()),
        ('name_char', T.StringType()),
        ('concept_blob', T.StringType()),
        ('update_date', T.TimestampType()),
        ('download_date', T.TimestampType()),
        ('import_date', T.TimestampType()),
        ('sourcesystem_cd', T.StringType()),
        ('upload_id', T.IntegerType()),
    ]),

    'observation_fact': OrderedDict([
        ('encounter_num', T.StringType()),
        ('concept_cd', T.StringType()),
        ('provider_id', T.StringType()),
        ('start_date', T.TimestampType()),
        ('patient_num', T.StringType()),
        ('modifier_cd', T.StringType()),
        ('instance_num', T.StringType()),
        ('valtype_cd', T.StringType()),
        ('tval_char', T.StringType()),
        ('nval_num', T.DecimalType(18, 5)),
        ('valueflag_cd', T.StringType()),
        ('quantity_num', T.DecimalType(18, 5)),
        ('units_cd', T.StringType()),
        ('end_date', T.TimestampType()),
        ('location_cd', T.StringType()),
        ('observation_blob', T.StringType()),
        ('confidence_num', T.DecimalType(18, 5)),
        ('update_date', T.TimestampType()),
        ('download_date', T.TimestampType()),
        ('import_date', T.TimestampType()),
        ('sourcesystem_cd', T.StringType()),
        ('upload_id', T.IntegerType()),
    ]),

    'patient_dimension': OrderedDict([
        ('patient_num', T.StringType()),
        ('vital_status_cd', T.StringType()),
        ('birth_date', T.TimestampType()),
        ('death_date', T.TimestampType()),
        ('sex_cd', T.StringType()),
        ('age_in_years_num', T.IntegerType()),
        ('language_cd', T.StringType()),
        ('race_cd', T.StringType()),
        ('marital_status_cd', T.StringType()),
        ('religion_cd', T.StringType()),
        ('zip_cd', T.StringType()),
        ('statecityzip_path', T.StringType()),
        ('patient_blob', T.StringType()),
        ('update_date', T.TimestampType()),
        ('download_date', T.TimestampType()),
        ('import_date', T.TimestampType()),
        ('sourcesystem_cd', T.StringType()),
        ('upload_id', T.IntegerType()),
    ]),

    'visit_dimension': OrderedDict([
        ('encounter_num', T.StringType()),
        ('patient_num', T.StringType()),
        ('active_status_cd', T.StringType()),
        ('start_date', T.TimestampType()),
        ('end_date', T.TimestampType()),
        ('inout_cd', T.StringType()),
        ('location_cd', T.StringType()),
        ('visit_blob', T.StringType()),
        ('update_date', T.TimestampType()),
        ('download_date', T.TimestampType()),
        ('import_date', T.TimestampType()),
        ('sourcesystem_cd', T.StringType()),
        ('upload_id', T.IntegerType()),
    ]),
}

required_domain_schema_dict = {
    'concept_dimension': OrderedDict([
        ('concept_path', T.StringType()),
        ('concept_cd', T.StringType()),
        ('name_char', T.StringType()),
    ]),

    'observation_fact': OrderedDict([
        ('encounter_num', T.StringType()),
        ('concept_cd', T.StringType()),
        ('provider_id', T.StringType()),
        ('start_date', T.TimestampType()),
        ('patient_num', T.StringType()),
        ('modifier_cd', T.StringType()),
        ('instance_num', T.StringType()),
    ]),

    'patient_dimension': OrderedDict([
        ('patient_num', T.StringType()),
    ]),

    'visit_dimension': OrderedDict([
        ('encounter_num', T.StringType()),
        ('patient_num', T.StringType()),
    ]),
}