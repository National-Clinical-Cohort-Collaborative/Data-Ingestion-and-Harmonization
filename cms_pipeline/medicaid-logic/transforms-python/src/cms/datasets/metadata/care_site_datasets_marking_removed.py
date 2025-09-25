from transforms.api import transform_df, Input, Output, Markings
from cms import configs


def make_transform(dataset, provider_col, person_col):
    input_dataset = configs.transform + "06_5 - add concept name/" + dataset
    @transform_df(
        Output(configs.metadata + dataset + "_marking_removed"),
        input_df=Input(input_dataset,
                    stop_propagating=Markings(["e7c7c550-c60a-4218-8cd0-99d0b41f8928"], on_branches=["master"])
            )
    )
    def compute_function(input_df):
        if dataset == "health_system":
            input_df = input_df.drop("health_system_hashed_id")

        return input_df

    return compute_function


datasets = [
    ("care_site", "care_site_id", None),
    ("health_system", "health_system_id", None)
]

transforms = (make_transform(*dataset) for dataset in datasets)
