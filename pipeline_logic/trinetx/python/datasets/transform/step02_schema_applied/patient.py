from pyspark.sql import types as T
from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from trinetx.utils import blanks_as_nulls
from trinetx.trinetx_schemas import complete_domain_schema_dict_string_type, schema_dict_all_string_type

domain = "patient"
required_schema = complete_domain_schema_dict_string_type[domain]
required_schema_lowercase = schema_dict_all_string_type(required_schema, all_lowercase=True)
required_schema_uppercase = schema_dict_all_string_type(required_schema)
schema_expectation = E.any(
    E.schema().contains(required_schema_lowercase),
    E.schema().contains(required_schema_uppercase)
)
output_checks = [
    Check(E.primary_key('PATIENT_ID'), 'Valid cleaned primary key', on_error='WARN')
]
input_checks = [
    Check(E.count().gt(0), 'Required TriNetX table is not empty', on_error='FAIL'),
    Check(schema_expectation, 'Dataset from site includes all expected columns', on_error='WARN'),
    Check(E.primary_key('PATIENT_ID'), 'Valid parsed primary key', on_error='WARN'),
    # Check(E.col('BIRTH_DATE').rlike("^[0-9]{4}-[0-9]{2}$"), 'YYYY-MM format for BIRTH_DATE column', on_error='WARN')
    # This check has been added through the Data Health UI so that it can be configured separately from the schema/empty table checks
]


@transform(
    processed=Output(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/02 - clean/patient',
        checks=output_checks
    ),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/01 - parsed/patient',
        checks=input_checks
    ),
)
def compute_function(my_input, processed):
    processed_df = my_input.dataframe()

    # Replace empty strings with nulls
    processed_df = blanks_as_nulls(processed_df)

    # Cast non-string columns to proper type
    processed_df = processed_df.withColumn("DEATH_DATE", processed_df["DEATH_DATE"].cast(T.DateType()))

    processed.write_dataframe(processed_df)
