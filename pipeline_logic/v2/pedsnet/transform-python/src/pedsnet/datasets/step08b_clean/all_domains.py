# from pyspark.sql import functions as F
from transforms.api import configure, transform_df, Input, Output
from pedsnet.anchor import path
from source_cdm_utils import clean


def make_transform(domain, profile):
    @configure(profile=profile)
    @transform_df(
        Output(path.transform + "08 - clean/" + domain),
        foundry_df=Input(path.transform + "07 - pre clean/processed/" + domain),
        concept=Input(path.concept)
    )
    def compute_function(foundry_df, concept, domain=domain):
        foundry_df = clean.conceptualize(domain, foundry_df, concept)
        return foundry_df

    return compute_function


domains = [
    ("care_site", None),
    ("condition_era", None),
    ("condition_occurrence", None),
    ("control_map", None),
    ("death", None),
    # ("device_exposure", None),
    ("dose_era", None),
    ("drug_era", None),
    ("drug_exposure", None),
    ("location", None),
    ("measurement", ["EXECUTOR_MEMORY_LARGE"]),
    # ("note", None),
    # ("note_nlp", None),
    ("observation", None),
    ("observation_period", None),
    # payer_plan_period
    ("person", None),
    ("procedure_occurrence", None),
    ("provider", None),
    # ("visit_detail", None),
    ("visit_occurrence", None),
]

transforms = (make_transform(*domain) for domain in domains)
