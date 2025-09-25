from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Markings
from cms import configs
from cms.utils import fixedSizeHash


def make_transform(dataset, provider_col, person_col):
    # exception for care_site, input must be care_site_marking_removed instead of step 04 output due to CMS Care Site Data
    # markings propagation issue
    # if dataset == "care_site":
    #     # RID for care_site_marking_removed dataset
    #     input_dataset = "/UNITE/[PPRL] CMS Data & Repository/pipeline/logic/cms/datasets/metadata/care_site_marking_removed"
    # else:
    input_dataset = configs.transform + "04 - domain_mapping/" + dataset

    @transform(
        output=Output(configs.transform + "05 - safe/" + dataset),
        input_df=Input(input_dataset),
        pepper=Input("ri.foundry.main.dataset.be49593a-56ce-4683-b126-ff667fe05be3"),
        opt_in=Input("ri.foundry.main.dataset.e0b86897-c6f0-4317-938d-2840fe0e2117", 
                stop_propagating=Markings(["032e4f9b-276a-48dc-9fb8-0557136ffe47"], on_branches=["master"])
        )
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

        # health_system_hash_id should not be removed even if it contains "hash"
        column_to_retain = "health_system_hash_id"

        # Select columns that do not contain "hash" in their names, or are the specific column to retain
        input_df = input_df.select(*[
            col for col in input_df.columns if "hash" not in col or col == column_to_retain
        ])

        if dataset == "observation" or dataset == 'measurement':
            input_df = input_df.withColumn("value_as_number", input_df["value_as_number"].cast("double"))

        input_df = input_df.withColumn("data_partner_id", F.lit("Medicare"))
        output.write_dataframe(input_df)

    return compute_function


datasets = [
    ("care_site", "care_site_id", None),
    ("condition_occurrence", "care_site_id", "person_id"),
    ("death", None, 'person_id'),
    ("device_exposure", None, "person_id"),
    ("drug_exposure", "care_site_id", "person_id"),
    ("health_system", "health_system_id", None),
    ("observation", "care_site_id", "person_id"),
    ("person", None, "person_id"),
    ("procedure_occurrence", "care_site_id", "person_id"),
    ("visit_occurrence", "care_site_id", "person_id"),
    ("visit_occurrence_with_macro_visit", "care_site_id", "person_id"),
    ("provider", "care_site_id", None),
    ("observation_period", None, "person_id"),
    ("condition_era", None, "person_id"),
    ("drug_era", None, "person_id"),
    ("measurement", "care_site_id", "person_id")
]

transforms = (make_transform(*dataset) for dataset in datasets)
