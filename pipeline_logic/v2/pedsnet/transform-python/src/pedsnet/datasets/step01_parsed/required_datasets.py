from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils.parse import required_parse
from pedsnet import local_schemas
from pedsnet.anchor import path


def make_transform(domain):
    schema_dict = local_schemas.complete_domain_schema_dict[domain]
    schema_cols = list(schema_dict.keys())

    checks = [
        Check(E.count().gt(0), 'Required dataset is not empty', 'FAIL')
    ]

    @transform(
        filename_input=Input(path.transform + "00 - unzipped/payload_filename"),
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/required/" + domain, checks=checks),
        error_output=Output(path.transform + "01 - parsed/errors/" + domain)
    )
    def compute_function(filename_input, payload_input, processed_output, error_output):
        required_parse(filename_input, payload_input, domain, processed_output, error_output, schema_cols, schema_cols)

    return compute_function


domains = [
    "condition_era",
    "condition_occurrence",
    "death",
    "drug_era",
    "drug_exposure",
    "location",
    "measurement",
    "observation",
    "observation_period",
    "person",
    "procedure_occurrence",
    "visit_occurrence",
]

transforms = (make_transform(domain) for domain in domains)
