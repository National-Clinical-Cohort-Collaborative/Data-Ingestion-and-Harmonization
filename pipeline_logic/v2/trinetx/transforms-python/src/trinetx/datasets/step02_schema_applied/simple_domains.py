from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from source_cdm_utils import schema
from trinetx import local_schema
from trinetx.anchor import path


def make_transform(domain, pkey, orphan_col, folder):
    schema_dict = local_schema.complete_domain_schema_dict[domain]
    required_dict = local_schema.required_domain_schema_dict[domain]
    schema_strings = schema.schema_dict_all_string_type(required_dict)
    schema_expectation = E.schema().contains(schema_strings)

    input_checks = [
        Check(schema_expectation, "`" + domain + "` schema must contain required columns", on_error='FAIL')
    ]

    output_checks = []
    if pkey:
        output_checks.append(Check(E.primary_key(pkey), '`' + pkey + '` must be a valid primary key', on_error='FAIL'))

    @transform(
        my_input=Input(path.transform + "01 - parsed/" + folder + "/" + domain, checks=input_checks),
        processed=Output(path.transform + '02 - clean/' + domain, checks=output_checks)
    )
    def compute_function(my_input, processed):
        processed_df = my_input.dataframe()

        # Drop "orphan" columns referring to patients not in the Patient table
        if orphan_col:
            processed_df = processed_df.filter(F.col(orphan_col) != "t")

        processed_df = schema.apply_schema(processed_df, schema_dict)

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    ("adt", None, None, "optional"),
    ("control_map", None, None, "optional"),
    ("diagnosis", None, "ORPHAN_FLAG", "required"),
    ("lab_result", None, "ORPHAN_FLAG", "required"),
    ("medication", None, "ORPHAN_FLAG", "required"),
    ("note", None, None, "cached"),
    ("note_nlp", None, None, "cached"),
    ("patient", "patient_id", None, "required"),
    ("procedure", None, "ORPHAN_FLAG", "required"),
    ("vital_signs", None, "ORPHAN_FLAG", "optional")
]

transforms = (make_transform(*domain) for domain in domains)
