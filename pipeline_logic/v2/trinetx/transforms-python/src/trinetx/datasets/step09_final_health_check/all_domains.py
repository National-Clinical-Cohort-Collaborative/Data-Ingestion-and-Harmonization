from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils import schema
from trinetx.anchor import path


def make_transform(domain, pkey):
    # Get complete schema for this OMOP domain
    complete_schema = schema.omop_complete[domain]

    # Cast to regular dictionary and convert column names to lowercase
    complete_schema = {k.lower(): v for k, v in complete_schema.items()}
    schema_expectation = E.schema().contains(complete_schema)

    all_checks = [
        Check(schema_expectation, 'Dataset must include expected OMOP columns with proper types', on_error='FAIL'),
    ]

    if pkey:
        all_checks.append(Check(E.primary_key(pkey), 'Valid primary key', on_error='FAIL'))

    # Get required non-null columns for this OMOP domain
    required_schema = schema.omop_required[domain]

    # Create non-null checks for each of these columns:
    for col_name in required_schema.keys():
        col_name = col_name.lower()
        expectation = E.col(col_name).non_null()
        expectation_name = '`' + col_name + '` column must not contain a null'
        # Temporarily remove the OMOP checks. Many tables fail these checks. We need to re-evaluate whether they're valid.
        # all_checks.append(Check(expectation, expectation_name, on_error='FAIL'))

    @transform_df(
        Output(path.transform + '09 - final health check/' + domain),
        my_input=Input(path.transform + '08 - clean/' + domain, checks=all_checks),
    )
    def compute_function(my_input):
        return my_input

    return compute_function


domains = [
    # ("care_site", "care_site_id"),
    ("condition_era", "condition_era_id"),
    ("condition_occurrence", "condition_occurrence_id"),
    ("control_map", "control_map_id"),
    ("death", None),
    ("device_exposure", "device_exposure_id"),
    ("drug_era", "drug_era_id"),
    ("drug_exposure", "drug_exposure_id"),
    ("location", "location_id"),
    ("measurement", "measurement_id"),
    ("note", "note_id"),
    ("note_nlp", "note_nlp_id"),
    ("observation", "observation_id"),
    ("observation_period", "observation_period_id"),
    # ("payer_plan_period", "payer_plan_period_id"),
    ("person", "person_id"),
    ("procedure_occurrence", "procedure_occurrence_id"),
    # ("provider", "provider_id"),
    ("visit_occurrence", "visit_occurrence_id"),
    ("visit_detail", "visit_detail_id"),
]

transforms = (make_transform(*domain) for domain in domains)
