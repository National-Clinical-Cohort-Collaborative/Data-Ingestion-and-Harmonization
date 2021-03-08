from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from pcornet.omop_schemas import complete_domain_schema_dict

domain = "death"
# Get complete schema for this OMOP domain as an OrderedDict
complete_schema = complete_domain_schema_dict[domain]
# Cast to regular dictionary and convert column names to lowercase
complete_schema = {k.lower(): v for k, v in complete_schema.items()}
schema_expectation = E.schema().contains(complete_schema)


@transform_df(
    Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/death'),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/06 - id generation/death',
        checks=[
            Check(schema_expectation, 'Dataset includes expected OMOP columns with proper types', on_error='WARN'),
            Check(E.col('person_id').non_null(), 'person_id column contains null', on_error='WARN'),
            Check(E.col('death_date').non_null(), 'death_date column contains null', on_error='WARN'),
            Check(E.col('death_type_concept_id').non_null(), 'death_type_concept_id column contains null', on_error='WARN'),
        ]
    ),
)
def compute_function(my_input):
    return my_input
