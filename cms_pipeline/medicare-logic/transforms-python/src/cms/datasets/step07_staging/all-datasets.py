
from transforms.api import transform, Input, Output
from cms import configs
from pyspark.sql import functions as F


# This step simply copies the domain tables from the final step of the transformation pipeline.
# This acts as a staging area for the domain tables to be combined with other sites.
# If any part of the transformation pipeline fails, then this step will not run.
# This prevents having some domains which succeed and some which fail for a new payload
# and ending up with inconsistent versions in the overall LDS dataset. It is a multi-output transform
# for the same reason - if any of the transactions fail, they all will fail together.
domains = [
    "care_site",
    "condition_occurrence",
    "death",
    "device_exposure",
    "drug_exposure",
    "health_system",
    "observation",
    "person",
    "procedure_occurrence",
    "visit_occurrence",
    "visit_occurrence_with_macrovisit",
    "provider",
    "observation_period",
    "condition_era",
    "drug_era",
    "measurement"
]


PRIMARY_KEY_COLUMNS = [
    'care_site_id',
    'case_person_id',
    'condition_era_id',
    'condition_occurrence_id',
    'control_map_id',
    'control_person_id',
    'device_exposure_id',
    'dose_era_id',
    'drug_era_id',
    'drug_exposure_id',
    'health_system_id',
    'location_id',
    'measurement_id',
    'note_id',
    'note_nlp_id',
    'observation_id',
    'observation_period_id',
    'payer_plan_period_id',
    'person_id',
    'preceding_visit_detail_id',
    'preceding_visit_occurrence_id',
    'procedure_occurrence_id',
    'provider_id',
    'visit_detail_id',
    'visit_detail_parent_id',
    'visit_occurrence_id',
]

# care_site and health_system have different input paths
# they need to have their special markings removed after step 6_5
exceptions = {
    "care_site": configs.root + "logic/cms/datasets/metadata/care_site_marking_removed",
    "health_system": configs.root + "logic/cms/datasets/metadata/health_system_marking_removed",
}

# Use the dictionary comprehension with a conditional check for exceptions
inputs = {
    domain + "_in": Input(exceptions[domain] if domain in exceptions else configs.transform + '06_5 - add concept names/' + domain)
    for domain in domains
}

outputs = {domain + "_out": Output(configs.staging + domain) for domain in domains}


@transform(**inputs, **outputs)
def my_compute_function(**all_dfs):
    for domain in domains:
        input_df = all_dfs[domain + "_in"].dataframe()

        for col in input_df.columns:  # noqa
            if col in PRIMARY_KEY_COLUMNS:
                input_df = input_df.withColumn(col,
                    F.concat(
                        F.lit("MEDICARE"),
                        input_df[col].cast("string")
                    )
                )
            if col == "cms_person_id":
                input_df = input_df.drop(col)

        if domain == "provider":
            # Drop most provider columns
            input_df = input_df.select(
                "provider_id",
                "specialty_concept_id",
                "specialty_source_value",
                "specialty_source_concept_id",
                "specialty_concept_name")

        if domain == "care_site":
            input_df = input_df.select(
                    "care_site_id",
                    "care_site_name",
                    "place_of_service_concept_id",
                    "location_id",
                    "care_site_source_value",
                    "place_of_service_source_value",
                    )

        if domain == "person":
            input_df = input_df.select(
                "person_id",
                "data_partner_id",
                "gender_concept_name",
                "ethnicity_concept_name",
                "race_concept_name",
                "gender_concept_id",
                "ethnicity_concept_id",
                "race_concept_id",
                "month_of_birth",
                "year_of_birth"
            )

        output_df = all_dfs[domain + "_out"]

        output_df.write_dataframe(input_df)
