from transforms.api import transform, Input, Output, configure
from source_cdm_utils.parse import cached_parse
from trinetx.anchor import path
from trinetx import local_schema


def make_transform(domain):
    schema_dict = local_schema.complete_domain_schema_dict[domain]
    schema_cols = list(schema_dict.keys())

    @configure(profile=['KUBERNETES_NO_EXECUTORS'])
    @transform(
        filename_input=Input(path.transform + "00 - unzipped/payload_filename"),
        payload_input=Input(path.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(path.transform + "01 - parsed/cached/" + domain),
        error_output=Output(path.transform + "01 - parsed/errors/" + domain)
    )
    def compute_function(filename_input, payload_input, processed_output, error_output):
        cached_parse(filename_input, payload_input, domain, processed_output, error_output, schema_cols, schema_cols)

    return compute_function


domains = ["note", "note_nlp"]

transforms = (make_transform(domain) for domain in domains)
