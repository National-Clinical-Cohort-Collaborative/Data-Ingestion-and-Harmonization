from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from pcornet.pcornet_schemas import complete_domain_schema_dict, required_domain_schema_dict, schema_dict_all_string_type
from pcornet.utils import apply_schema

domain = "obs_gen"
# Get complete PCORnet schema
table_schema = complete_domain_schema_dict[domain]
table_schema_pcornet_v5 = complete_domain_schema_dict["obs_gen_5.0"]
# Get required PCORnet columns -- handle PCORnet 5.0 and 6.0 schemas
required_schema = required_domain_schema_dict[domain]
required_schema_pcornet_v5 = required_domain_schema_dict["obs_gen_5.0"]
# Check that required columns are present, but they will all be Strings
# Handle all lowercase or all uppercase column names
required_schema_lowercase = schema_dict_all_string_type(required_schema, all_lowercase=True)
required_schema_uppercase = schema_dict_all_string_type(required_schema)
required_schema_lowercase_v5 = schema_dict_all_string_type(required_schema_pcornet_v5, all_lowercase=True)
required_schema_uppercase_v5 = schema_dict_all_string_type(required_schema_pcornet_v5)
schema_expectation = E.any(
    E.schema().contains(required_schema_lowercase),
    E.schema().contains(required_schema_uppercase),
    E.schema().contains(required_schema_lowercase_v5),
    E.schema().contains(required_schema_uppercase_v5)
)


@transform(
    processed=Output(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/02 - clean/obs_gen',
        checks=[
            Check(E.primary_key('obsgenid'), 'Valid cleaned primary key', on_error='WARN')
        ]
    ),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/01 - parsed/obs_gen',
        checks=[
            Check(schema_expectation, 'Dataset from site includes all expected columns', on_error='WARN'),
            Check(E.any(E.primary_key('OBSGENID'), E.primary_key('obsgenid')), 'Valid parsed primary key', on_error='WARN')
        ]
    ),
)
def compute_function(my_input, processed):
    processed_df = my_input.dataframe()

    # Handle PCORnet 5.0 schema (containing obsgen_date and obsgen_time columns)
    lower_cols = [col.lower() for col in processed_df.columns]
    if "obsgen_date" in lower_cols:
        processed_df = apply_schema(processed_df, table_schema_pcornet_v5)
    else:
        processed_df = apply_schema(processed_df, table_schema)

    processed.write_dataframe(processed_df)
