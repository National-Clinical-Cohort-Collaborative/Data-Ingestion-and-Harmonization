from transforms.api import configure, transform, Input, Output, Check
from transforms import expectations as E
from myproject.local_utils import get_domain_id
from myproject.anchor import path
from source_cdm_utils import schema
from myproject import local_schemas


def make_transform(domain, pkey, concept_col, folder, profile):
    # Get complete OMOP schema
    schema_dict = local_schemas.complete_domain_schema_dict[domain]
    required_dict = local_schemas.required_domain_schema_dict[domain]
    schema_strings = schema.schema_dict_all_string_type(required_dict, add_payload=True)

    input_checks = [
        Check(E.schema().contains(schema_strings), "`" + domain + "` schema must contain required columns", "FAIL")
    ]

    output_checks = []
    if pkey:
        output_checks.append(Check(E.primary_key(pkey), '`' + pkey + '` must be a valid primary key', on_error='FAIL'))

    @configure(profile=profile)
    @transform(
        processed=Output(path.transform + "02 - clean/" + domain, checks=output_checks),
        my_input=Input(path.transform + "01 - parsed/" + folder + "/" + domain, checks=input_checks),
        site_id_df=Input(path.site_id),
        concept=Input(path.concept),
    )
    def compute_function(my_input, site_id_df, processed, concept):
        processed_df = my_input.dataframe()
        processed_df = schema.apply_schema(processed_df, schema_dict)
        processed_df = schema.add_site_id_col(processed_df, site_id_df)

        if concept_col:
            processed_df = get_domain_id(processed_df, concept.dataframe(), concept_col)

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    # domain, pkey, concept_col, folder, profile
    ("care_site", "care_site_id", None, "optional", None),
    ("condition_era", "condition_era_id", "condition_concept_id", "required", None),
    ("condition_occurrence", "condition_occurrence_id", "condition_concept_id", "required", None),
    ("control_map", None, None, "optional", None),
    ("death", None, None, "required", None),
    ("device_exposure", "device_exposure_id", "device_concept_id", "optional", None),
    ("dose_era", "dose_era_id", None, "optional", None),
    ("drug_era", "drug_era_id", "drug_concept_id", "required", None),
    ("drug_exposure", "drug_exposure_id", "drug_concept_id", "required", None),
    ("location", "location_id", None, "required", None),
    ("measurement", "measurement_id", "measurement_concept_id", "required", "EXECUTOR_MEMORY_LARGE"),
    ("note", None, None, "cached", None),  # Temporarily do not check the primary key - note_id
    ("note_nlp", None, None, "cached", None),  # Temporarily do not check the primary key - note_nlp_id
    ("observation", "observation_id", "observation_concept_id", "required", None),
    ("observation_period", "observation_period_id", None, "required", None),
    ("person", "person_id", None, "required", None),
    ("procedure_occurrence", "procedure_occurrence_id", "procedure_concept_id", "required", None),
    ("provider", "provider_id", None, "optional", None),
    ("visit_detail", "visit_detail_id", "visit_detail_concept_id", "optional", None),
    ("visit_occurrence", "visit_occurrence_id", None, "required", None),
]

transforms = (make_transform(*domain) for domain in domains)
