from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils import schema
from trinetx import local_schema
from trinetx.anchor import path

domain = "encounter"
pkey = "encounter_id"
folder = "required"

schema_dict = local_schema.complete_domain_schema_dict[domain]
required_dict = local_schema.required_domain_schema_dict[domain]
schema_strings = schema.schema_dict_all_string_type(required_dict)
schema_expectation = E.schema().contains(schema_strings)

input_checks = [
    Check(schema_expectation, "`" + domain + "` schema must contain required columns", on_error='FAIL')
]

output_checks = []
if pkey:
    output_checks.append(Check(E.primary_key(pkey), '`' + pkey + '` must be a valid primary key', on_error='FAIL'))


@transform(
    my_input=Input(path.transform + "01 - parsed/" + folder + "/" + domain, checks=input_checks),
    processed=Output(path.transform + '02 - clean/' + domain, checks=output_checks),
    duplicates=Output(path.metadata + 'duplicate_encounter_ids'),
)
def compute_function(my_input, processed, duplicates):
    processed_df = my_input.dataframe()

    # Drop "orphan" columns referring to patients not in the Patient table
    processed_df = processed_df.filter(F.col("ORPHAN_FLAG") != "t")

    processed_df = schema.apply_schema(processed_df, schema_dict)

    # Drop duplicate primary keys but log them to output dataset as a warning
    # Valid encounter_ids are either (1) unique or (2) a pair of IDs that are both of type EI
    unique_ids = processed_df.groupBy("encounter_id").count().where(F.col("count") == 1).select("encounter_id")

    valid_double_ids = processed_df.groupBy("encounter_id", "encounter_type").count().where(
        (F.upper(F.col("encounter_type")) == "EI") & (F.col("count") == 2)
        ).select("encounter_id")

    all_valid_ids = unique_ids.unionByName(valid_double_ids).distinct()

    valid_encounters = processed_df.join(all_valid_ids, "encounter_id", "inner")
    duplicate_encounters = processed_df.join(all_valid_ids, "encounter_id", "left_anti")

    processed.write_dataframe(valid_encounters)
    duplicates.write_dataframe(duplicate_encounters)
