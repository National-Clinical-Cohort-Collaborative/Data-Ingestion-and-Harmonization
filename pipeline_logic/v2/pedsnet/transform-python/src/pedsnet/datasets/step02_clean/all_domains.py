from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from pedsnet.anchor import path
from source_cdm_utils import schema
from pedsnet import local_schemas


def make_transform(domain, pkey, folder):
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

    @transform(
        processed=Output(path.transform + "02 - clean/" + domain, checks=output_checks),
        my_input=Input(path.transform + "01 - parsed/" + folder + "/" + domain, checks=input_checks),
        site_id_df=Input(path.site_id),
    )
    def compute_function(my_input, site_id_df, processed):

        processed_df = my_input.dataframe()
        processed_df = schema.apply_schema(processed_df, schema_dict)
        processed_df = schema.add_site_id_col(processed_df, site_id_df)

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    # domain, pkey, folder
    ("care_site", "care_site_id", "optional"),
    ("condition_era", "condition_era_id", "required"),
    ("condition_occurrence", "condition_occurrence_id", "required"),
    ("control_map", None, "optional"),
    ("death", None, "required"),
    ("device_exposure", "device_exposure_id", "optional"),
    ("dose_era", "dose_era_id", "optional"),
    ("drug_era", "drug_era_id", "required"),
    ("drug_exposure", "drug_exposure_id", "required"),
    ("location", "location_id", "required"),
    ("measurement", "measurement_id", "required"),
    # ("note", None, "cached"),  # Temporarily do not check the primary key - note_id
    # ("note_nlp", None, "cached"),  # Temporarily do not check the primary key - note_nlp_id
    ("observation", "observation_id", "required"),
    ("observation_period", "observation_period_id", "required"),
    ("person", "person_id", "required"),
    ("procedure_occurrence", "procedure_occurrence_id", "required"),
    ("provider", "provider_id", "optional"),
    ("visit_detail", "visit_detail_id", "optional"),
    ("visit_occurrence", "visit_occurrence_id", "required"),
]

transforms = (make_transform(*domain) for domain in domains)
