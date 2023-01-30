from transforms.api import transform, Input, Output
from trinetx.anchor import path
from source_cdm_utils import pre_clean


def make_transform(domain):
    @transform(
        processed=Output(path.transform + "07 - pre clean/processed/" + domain),
        nulled_rows=Output(path.transform + "07 - pre clean/nulled/" + domain),
        removed_rows=Output(path.transform + "07 - pre clean/removed/" + domain),

        foundry_df=Input(path.transform + "06 - id generation/" + domain),
        removed_person_ids=Input(path.transform + "07 - pre clean/pre_clean_removed_person_ids"),

        ahrq_xwalk=Input("/UNITE/LDS/AHRQ Safety Measure/ahrq_code_xwalk"),
        tribal_zips=Input("/UNITE/AOU Tribal Zip Codes/Tribal Zip Codes"),
        loincs_to_remove=Input("/UNITE/LDS/LOINC Codes to Remove/loinc_codes_to_remove")
    )
    def compute_function(
        processed, nulled_rows, removed_rows,
        foundry_df, removed_person_ids,
        ahrq_xwalk, tribal_zips, loincs_to_remove, ctx
    ):
        pre_clean.do_pre_clean(
            domain,
            processed, nulled_rows, removed_rows,
            foundry_df, removed_person_ids,
            ahrq_xwalk, tribal_zips, loincs_to_remove, ctx)

    return compute_function


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

transforms = (make_transform(domain) for domain in domains)
