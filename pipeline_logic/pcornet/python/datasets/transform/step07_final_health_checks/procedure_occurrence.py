from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from pcornet.omop_schemas import complete_domain_schema_dict

domain = "procedure_occurrence"
# Get complete schema for this OMOP domain as an OrderedDict
complete_schema = complete_domain_schema_dict[domain]
# Cast to regular dictionary and convert column names to lowercase
complete_schema = {k.lower(): v for k, v in complete_schema.items()}
schema_expectation = E.schema().contains(complete_schema)


@transform_df(
    Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/procedure_occurrence'),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/06 - id generation/procedure_occurrence',
        checks=[
            Check(E.primary_key('procedure_occurrence_id'), 'Valid primary key', on_error='FAIL'),
            Check(schema_expectation, 'Dataset includes expected OMOP columns with proper types', on_error='WARN'),
            Check(E.col('procedure_occurrence_id').non_null(), 'procedure_occurrence_id column contains null', on_error='WARN'),
            Check(E.col('person_id').non_null(), 'person_id column contains null', on_error='WARN'),
            Check(E.col('procedure_concept_id').non_null(), 'procedure_concept_id column contains null', on_error='WARN'),
            Check(E.col('procedure_date').non_null(), 'procedure_date column contains null', on_error='WARN'),
            Check(E.col('procedure_type_concept_id').non_null(), 'procedure_type_concept_id column contains null', on_error='WARN'),
        ]
    ),
)
def compute_function(my_input):
    return my_input
