from transforms.api import transform, Input, Output
from source_cdm_utils.parse import optional_parse
from trinetx.anchor import path
from trinetx import local_schema


def make_transform(domain):
    schema_dict = local_schema.complete_domain_schema_dict[domain]
    schema_cols = list(schema_dict.keys())

    @transform(
        filename_input=Input(path.transform + "00 - unzipped/payload_filename"),
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/optional/" + domain),
        error_output=Output(path.transform + "01 - parsed/errors/" + domain)
    )
    def compute_function(filename_input, payload_input, processed_output, error_output):
        optional_parse(filename_input, payload_input, domain, processed_output, error_output, schema_cols, schema_cols)

    return compute_function


domains = ["adt", "control_map", "vital_signs"]

transforms = (make_transform(domain) for domain in domains)
