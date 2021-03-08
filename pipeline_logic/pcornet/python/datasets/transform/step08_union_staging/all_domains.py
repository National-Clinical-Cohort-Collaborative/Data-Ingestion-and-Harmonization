from transforms.api import transform, Input, Output

# This step simply copies the domain tables from the final/ step of the transformation pipeline.
# This acts as a staging area for the domain tables to be combined with other sites.
# If any part of the transformation pipeline fails, then this step will not run.
# This prevents having some domains which succeed and some which fail for a new payload
# and ending up with inconsistent versions in the overall LDS dataset.

# Filepaths listed explicitly so that template will replace them for each site
inputs = {
    "person": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/person'),
    "condition_occurrence": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/condition_occurrence'),
    "observation": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/observation'),
    "drug_exposure": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/drug_exposure'),
    "location": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/location'),
    "death": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/death'),
    "condition_era": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/condition_era'),
    "procedure_occurrence": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/procedure_occurrence'),
    "drug_era": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/drug_era'),
    "observation_period": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/observation_period'),
    "visit_occurrence": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/visit_occurrence'),
    "measurement": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/measurement'),
    "provider": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/provider'),
    "care_site": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/care_site'),
    "device_exposure": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/device_exposure'),
    "payer_plan_period": Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/final/payer_plan_period'),
}
outputs = {
    "person_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/person'),
    "condition_occurrence_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/condition_occurrence'),
    "observation_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/observation'),
    "drug_exposure_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/drug_exposure'),
    "location_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/location'),
    "death_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/death'),
    "condition_era_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/condition_era'),
    "procedure_occurrence_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/procedure_occurrence'),
    "drug_era_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/drug_era'),
    "observation_period_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/observation_period'),
    "visit_occurrence_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/visit_occurrence'),
    "measurement_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/measurement'),
    "provider_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/provider'),
    "care_site_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/care_site'),
    "device_exposure_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/device_exposure'),
    "payer_plan_period_out": Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/union_staging/payer_plan_period'),
}
all_dfs = {**inputs, **outputs}


@transform(**all_dfs)
def my_compute_function(**all_dfs):
    for domain in inputs.keys():
        input_df = all_dfs[domain]
        input_df = input_df.dataframe()
        output_df = all_dfs[domain + "_out"]
        output_df.write_dataframe(input_df)
