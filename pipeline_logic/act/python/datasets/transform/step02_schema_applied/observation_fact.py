from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from act.act_schemas import complete_domain_schema_dict, required_domain_schema_dict, schema_dict_all_string_type
from act.utils import apply_schema

domain = "observation_fact"
# Get complete ACT schema
table_schema = complete_domain_schema_dict[domain]
# Get required ACT columns
required_schema = required_domain_schema_dict[domain]
# Check that required columns are present, but they will all be Strings
# Handle all lowercase or all uppercase column names
required_schema_lowercase = schema_dict_all_string_type(required_schema)
required_schema_uppercase = schema_dict_all_string_type(required_schema, all_uppercase=True)
schema_expectation = E.any(
    E.schema().contains(required_schema_lowercase),
    E.schema().contains(required_schema_uppercase)
)


@transform(
    processed=Output(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/02 - clean/observation_fact',
        checks=[
            Check(
                E.primary_key('encounter_num', 'concept_cd', 'provider_id', 'start_date', 'patient_num', 'modifier_cd', 'instance_num'),
                'Valid cleaned primary key columns', on_error='WARN'
            )
        ]
    ),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/01 - parsed/observation_fact',
        checks=[
            Check(E.count().gt(0), 'Required ACT table is not empty', on_error='FAIL'),
            Check(schema_expectation, 'Dataset from site includes all expected columns', on_error='WARN'),
            Check(E.any(
                E.primary_key('encounter_num', 'concept_cd', 'provider_id', 'start_date', 'patient_num', 'modifier_cd', 'instance_num'),
                E.primary_key('ENCOUNTER_NUM', 'CONCEPT_CD', 'PROVIDER_ID', 'START_DATE', 'PATIENT_NUM', 'MODIFIER_CD', 'INSTANCE_NUM')
                ), 'Valid parsed primary key columns', on_error='WARN'
            )
        ]
    ),
)
def compute_function(my_input, processed):

    processed_df = my_input.dataframe()
    processed_df = apply_schema(processed_df, table_schema)

    processed.write_dataframe(processed_df)
