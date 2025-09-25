from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Markings
from cms import configs
from cms.utils import fixedSizeHash


def make_transform(dataset, provider_col, person_col):

    input_dataset = dataset

    @transform(
        output=Output(configs.transform + "05 - safe/" + dataset),
        input_df=Input(configs.transform + "04 - domain_mapping/" + input_dataset),
        pepper=Input("ri.foundry.main.dataset.be49593a-56ce-4683-b126-ff667fe05be3"),
        opt_in=Input(configs.metadata + "cms_opt_in_only",
            stop_propagating=Markings(["032e4f9b-276a-48dc-9fb8-0557136ffe47", "5e003460-bb7d-4553-9049-922ad376eb23"],
            on_branches=["master"]))
    )
    def compute_function(input_df, output, pepper, opt_in):
        input_df = input_df.dataframe()
        opt_in = opt_in.dataframe()
        opt_in = opt_in.select("cms_person_id").distinct()
        pepper = pepper.dataframe()
        pepper_person = pepper.filter(pepper.Variable == "pepper_person").collect()[0].UUID  # noqa
        pepper_provider = pepper.filter(pepper.Variable == "pepper_provider").collect()[0].UUID  # noqa

        if provider_col is not None:
            input_df = input_df.withColumn(provider_col, fixedSizeHash(F.col(provider_col), pepper_provider, 59))

        if person_col is not None:
            input_df = input_df.join(opt_in, F.col("cms_person_id") == F.col(person_col), "inner")
            input_df = input_df.withColumn(person_col, fixedSizeHash(F.col(person_col), pepper_person, 59))

        if dataset == "location":
            input_df = input_df.withColumn("location_id", fixedSizeHash(F.col("location_id"), pepper_person, 59))

        # health_system_hash_id should not be removed even if it contains "hash"
        column_to_retain = "health_system_hashed_id"

        # Select columns that do not contain "hash" in their names, or are the specific column to retain
        input_df = input_df.select(*[
            col for col in input_df.columns if "hash" not in col or col == column_to_retain
        ])

        # occurs in observation, measurement
        if "value_as_number" in input_df.columns:
            input_df = input_df.withColumn("value_as_number", input_df.value_as_number.cast("double"))

        if dataset == "device_exposure":
            input_df = input_df.withColumn("provider_id", input_df.provider_id.cast("long"))

        if dataset == "death" or dataset == "person":
            input_df = input_df.drop("person_id")
            input_df = input_df.withColumnRenamed("medicaid_person_id", "person_id")

        if dataset == "care_site":
            input_df = input_df.drop("care_site_npi")

        input_df = input_df.withColumn("data_partner_id", F.lit("Medicaid"))
        output.write_dataframe(input_df)

    return compute_function


datasets = [
    ("care_site", "care_site_id", None),
    ("condition_occurrence", "provider_id", "person_id"),
    ("death", None, 'medicaid_person_id'),
    ("device_exposure", None, "person_id"),
    ("drug_exposure", "provider_id", "person_id"),
    ("health_system", "health_system_id", None),
    ("location", None, None),
    ("observation", "provider_id", "person_id"),
    ("person", None, "medicaid_person_id"),
    ("procedure_occurrence", "provider_id", "person_id"),
    ("visit_occurrence", "provider_id", "person_id"),
    ("provider", "provider_id", None),
    ("observation_period", None, "person_id"),
    ("condition_era", None, "person_id"),
    ("drug_era", None, "person_id"),
    ("measurement", "provider_id", "person_id")
]

transforms = (make_transform(*dataset) for dataset in datasets)
