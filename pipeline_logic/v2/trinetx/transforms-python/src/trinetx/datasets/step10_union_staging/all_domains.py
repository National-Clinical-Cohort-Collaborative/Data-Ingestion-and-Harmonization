from transforms.api import transform, Input, Output
from trinetx.anchor import path


# This step simply copies the domain tables from the final step of the transformation pipeline.
# This acts as a staging area for the domain tables to be combined with other sites.
# If any part of the transformation pipeline fails, then this step will not run.
# This prevents having some domains which succeed and some which fail for a new payload
# and ending up with inconsistent versions in the overall LDS dataset. It is a multi-output transform
# for the same reason - if any of the transactions fail, they all will fail together.
domains = [
    # "care_site",
    "condition_era",
    "condition_occurrence",
    "control_map",
    "death",
    "device_exposure",
    # dose_era
    "drug_era",
    "drug_exposure",
    "location",
    "measurement",
    "note",
    "note_nlp",
    "observation",
    "observation_period",
    # "payer_plan_period",
    "person",
    "procedure_occurrence",
    # "provider",
    "visit_occurrence",
    "visit_detail"
]

inputs = {domain + "_in": Input(path.transform + '09 - final health check/' + domain) for domain in domains}
outputs = {domain + "_out": Output(path.union_staging + "staged/" + domain) for domain in domains}


@transform(**inputs, **outputs)
def my_compute_function(**all_dfs):
    for domain in domains:
        input_df = all_dfs[domain + "_in"].dataframe()
        output_df = all_dfs[domain + "_out"]

        output_df.write_dataframe(input_df)
