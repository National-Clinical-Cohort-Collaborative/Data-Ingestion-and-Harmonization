from transforms.api import transform_df, Input, Output
from transforms.verbs import dataframes as D
from cms import configs


def make_transform(dataset):
    if dataset.melting:
        input_dfs = {
            group: Input(configs.transform + "03 - melted/initial/" + dataset.name + "_" + group) for group in dataset.melting
        }

        @transform_df(
            Output(configs.transform + "03 - melted/datasets/" + dataset.name),
            **input_dfs
        )
        def compute_function(**input_dfs):
            return D.union_many(*input_dfs.values())

        return compute_function


transforms = filter(lambda transform: transform, (make_transform(dataset) for dataset in configs.datasets))
