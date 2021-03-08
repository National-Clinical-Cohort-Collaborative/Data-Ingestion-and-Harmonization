from pyspark.sql import types as T
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from trinetx.utils import blanks_as_nulls
from trinetx.trinetx_schemas import complete_domain_schema_dict_string_type, schema_dict_all_string_type

domain = "encounter"
required_schema = complete_domain_schema_dict_string_type[domain]
required_schema_lowercase = schema_dict_all_string_type(required_schema, all_lowercase=True)
required_schema_uppercase = schema_dict_all_string_type(required_schema)
schema_expectation = E.any(
    E.schema().contains(required_schema_lowercase),
    E.schema().contains(required_schema_uppercase)
)
input_checks = [
    Check(E.count().gt(0), 'Required TriNetX table is not empty', on_error='FAIL'),
    Check(schema_expectation, 'Dataset from site includes all expected columns', on_error='WARN'),
]


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/02 - clean/encounter'),
    duplicates=Output(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/02 - clean/duplicate_encounter_ids',
        # checks=Check(E.count().equals(0), 'Encounter records with non-unique primary keys', on_error='WARN')
        # This check has been added through the Data Health UI so that it can be configured separately from the schema/empty table checks
    ),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/encounter',
        checks=input_checks
    ),
)
def compute_function(my_input, processed, duplicates):

    processed_df = my_input.dataframe()

    # Replace empty strings with nulls
    processed_df = blanks_as_nulls(processed_df)

    # Drop "orphan" columns referring to patients not in the Patient table
    processed_df = processed_df.filter(col("ORPHAN_FLAG") != "t")

    # Cast non-string columns to proper type
    processed_df = processed_df.withColumn("START_DATE", processed_df["START_DATE"].cast(T.DateType()))
    processed_df = processed_df.withColumn("END_DATE", processed_df["END_DATE"].cast(T.DateType()))
    processed_df = processed_df.withColumn("LENGTH_OF_STAY", processed_df["LENGTH_OF_STAY"].cast(T.IntegerType()))
    processed_df = processed_df.withColumn("ORPHAN_FLAG", when(col("ORPHAN_FLAG") == 'f', False).cast(T.BooleanType()))

    # Drop duplicate primary keys but log them to output dataset as a warning
    # Valid encounter_ids are either (1) unique or (2) a pair of IDs that are both of type EI
    unique_ids = processed_df.groupBy("ENCOUNTER_ID").count().where(
        F.col("count") == 1
        ).select("ENCOUNTER_ID")
    valid_double_ids = processed_df.groupBy("ENCOUNTER_ID", "ENCOUNTER_TYPE").count().where(
        (F.upper(F.col("ENCOUNTER_TYPE")) == "EI") & (F.col("count") == 2)
        ).select("ENCOUNTER_ID")
    all_valid_ids = unique_ids.unionByName(valid_double_ids).distinct()

    valid_encounters = processed_df.join(all_valid_ids, "ENCOUNTER_ID", "inner")
    duplicate_encounters = processed_df.join(all_valid_ids, "ENCOUNTER_ID", "left_anti")

    processed.write_dataframe(valid_encounters)
    duplicates.write_dataframe(duplicate_encounters)
