from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from pcornet.pcornet_schemas import complete_domain_schema_dict, required_domain_schema_dict, schema_dict_all_string_type
from pcornet.utils import apply_schema

domain = "provider"
# Get complete PCORnet schema
table_schema = complete_domain_schema_dict[domain]
# Get required PCORnet columns
required_schema = required_domain_schema_dict[domain]
# Check that required columns are present, but they will all be Strings
# Handle all lowercase or all uppercase column names
required_schema_lowercase = schema_dict_all_string_type(required_schema, all_lowercase=True)
required_schema_uppercase = schema_dict_all_string_type(required_schema)
schema_expectation = E.any(
    E.schema().contains(required_schema_lowercase),
    E.schema().contains(required_schema_uppercase)
)


@transform(
    processed=Output(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/02 - clean/provider',
        checks=[
            Check(E.primary_key('providerid'), 'Valid cleaned primary key', on_error='WARN')
        ]
    ),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/provider',
        checks=[
            Check(schema_expectation, 'Dataset from site includes all expected columns', on_error='WARN'),
            Check(E.any(E.primary_key('PROVIDERID'), E.primary_key('providerid')), 'Valid parsed primary key', on_error='WARN')
        ]
    ),
)
def compute_function(my_input, processed):

    processed_df = my_input.dataframe()
    processed_df = apply_schema(processed_df, table_schema)

    processed.write_dataframe(processed_df)
