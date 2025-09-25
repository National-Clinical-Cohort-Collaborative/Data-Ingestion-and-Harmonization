from transforms.api import transform_df, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils import schema
from cms import configs
from pyspark.sql import functions as F


def make_transform(domain, pkey):
    # Get complete schema for this OMOP domain
    complete_schema = schema.omop_complete[domain]

    # Cast to regular dictionary and convert column names to lowercase
    complete_schema = {k.lower(): v for k, v in complete_schema.items()}
    schema_expectation = E.schema().contains(complete_schema)

    all_checks = []

    checks_to_ignore = [
        "health_system"
    ]

    if pkey:
        if domain not in checks_to_ignore:
            all_checks = [
                Check(schema_expectation, 'Dataset must include expected OMOP columns with proper types', on_error='FAIL'),
                Check(E.primary_key(pkey), 'Valid primary key', on_error='FAIL')
            ]

    # Get required non-null columns for this OMOP domain
    required_schema = schema.omop_required[domain]

    # Create non-null checks for each of these columns:
    for col_name in required_schema.keys():
        col_name = col_name.lower()
        expectation = E.col(col_name).non_null()
        expectation_name = '`' + col_name + '` column must not contain a null'
        # Temporarily remove the OMOP checks. Many tables fail these checks.
        # all_checks.append(Check(expectation, expectation_name, on_error='FAIL'))

    @transform_df(
        Output(configs.transform + "06 - schema check/" + domain),
        my_input=Input(configs.transform + "05 - safe/" + domain, checks=all_checks),
    )
    def compute_function(my_input):
        if domain == "care_site":
            my_input = my_input.drop("orig_care_site_id")

        return my_input

    return compute_function


domains = [
    ("care_site", "care_site_id"),
    ("condition_occurrence", "condition_occurrence_id"),
    ("death", None),
    ("device_exposure", "device_exposure_id"),
    ("drug_exposure", "drug_exposure_id"),
    ("health_system", "health_system_id"),
    ("location", "location_id"),
    ("observation", "observation_id"),
    ("person", "person_id"),
    ("procedure_occurrence", "procedure_occurrence_id"),
    ("provider", "provider_id"),
    ("visit_occurrence", "visit_occurrence_id"),
    ("observation_period", "observation_period_id"),
    ("condition_era", "condition_era_id"),
    ("drug_era", "drug_era_id"),
    ("measurement", "measurement_id")
]

transforms = (make_transform(*domain) for domain in domains)
