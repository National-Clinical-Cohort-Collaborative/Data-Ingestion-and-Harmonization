from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from myproject.schemas import complete_domain_schema_dict, required_domain_schema_dict
from myproject.utils import drop_nulls_from_selected_required_cols

domain = "condition_era"
# Get complete schema for this OMOP domain as an OrderedDict
complete_schema = complete_domain_schema_dict[domain]
# Cast to regular dictionary and convert column names to lowercase
complete_schema = {k.lower(): v for k, v in complete_schema.items()}
schema_expectation = E.schema().contains(complete_schema)

all_checks = [
    Check(E.primary_key('condition_era_id'), 'Valid global id primary key', on_error='FAIL'),
    Check(schema_expectation, 'Dataset includes expected OMOP columns with proper types', on_error='WARN'),
]

# Get required non-null columns for this OMOP domain as an OrderedDict
required_schema = required_domain_schema_dict[domain]
# Create non-null checks for each of these columns:
for col_name in required_schema.keys():
    col_name = col_name.lower()
    expectation = E.col(col_name).non_null()
    expectation_name = '{col_name} column is non-null'.format(col_name=col_name)
    all_checks.append(Check(expectation, expectation_name, on_error='WARN'))


@transform_df(
    Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/final/condition_era'),
    my_input=Input(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/06 - global id generation/condition_era',
        checks=all_checks
    ),
)
def compute_function(my_input):
    my_input = drop_nulls_from_selected_required_cols(my_input, domain)
    return my_input
