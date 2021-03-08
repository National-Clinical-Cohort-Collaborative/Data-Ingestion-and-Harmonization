from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from myproject.utils import apply_schema, add_site_id_col
from myproject.schemas import complete_domain_schema_dict, required_domain_schema_dict, schema_dict_all_string_type

domain = "location"
# Get complete OMOP schema
table_schema = complete_domain_schema_dict[domain]
# Get required OMOP columns
required_schema = required_domain_schema_dict[domain]
# Check that required columns are present, but they will all be Strings
# Handle all lowercase or all uppercase column names
required_schema_lowercase = schema_dict_all_string_type(required_schema, all_lowercase=True)
required_schema_uppercase = schema_dict_all_string_type(required_schema)
schema_expectation = E.any(
    E.schema().contains(required_schema_lowercase),
    E.schema().contains(required_schema_uppercase)
)

output_checks = [
    Check(E.primary_key('location_id'), 'Valid cleaned primary key', on_error='WARN')
]
input_checks = [
    Check(E.count().gt(0), 'Required OMOP table is not empty', on_error='WARN'),
    Check(schema_expectation, 'Schema contains required OMOP fields', on_error='WARN'),
    Check(E.any(E.primary_key(domain.upper()+"_ID"), E.primary_key(domain.lower()+"_id")), 'Valid parsed primary key', on_error='WARN')
]


@transform(
    processed=Output(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/02 - clean/location",
        checks=output_checks
    ),
    my_input=Input(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/01 - parsed/location",
        checks=input_checks
    ),
    site_id_df=Input("/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 25")
)
def compute_function(my_input, site_id_df, processed):

    processed_df = my_input.dataframe()
    processed_df = apply_schema(processed_df, table_schema)
    processed_df = add_site_id_col(processed_df, site_id_df)

    processed.write_dataframe(processed_df)
