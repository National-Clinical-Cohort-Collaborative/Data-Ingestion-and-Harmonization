from transforms.api import transform_df, Input, Output
from myproject.anchor import path
from source_cdm_utils import person_ids

domains_with_person_id_column = [
    "condition_era",
    "condition_occurrence",
    "death",
    "drug_era",
    "drug_exposure",
    "device_exposure",
    "measurement",
    "note",
    "observation",
    "observation_period",
    # "payer_plan_period",
    "person",
    "procedure_occurrence",
    "visit_detail",
    "visit_occurrence"
]

input_dfs = {domain: Input(path.transform + "05 - global id generation/" + domain) for domain in domains_with_person_id_column}


@transform_df(
    Output(path.transform + "06 - pre clean/pre_clean_removed_person_ids"),
    **input_dfs
)
def compute_function(ctx, person, **input_dfs):
    return person_ids.person_id_pre_clean(ctx, person, **input_dfs)
