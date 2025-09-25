from transforms.api import transform_df, Input, Output
from transforms.verbs import dataframes as D
from cms import configs


def make_groups(datasets):
    groups = {}
    for dataset in datasets:
        for groupName in dataset.melting:
            if groupName not in groups:
                groups[groupName] = []
            groups[groupName].append(dataset.name)

    return groups.items()


def make_transform(groupName, datasetNames):
    input_dfs = {
        name: Input(configs.transform + "03 - melted/initial/" + name + "_" + groupName) for name in datasetNames
    }

    @transform_df(
        Output(configs.transform + "03 - melted/groups/" + groupName),
        **input_dfs
    )
    def compute_function(**input_dfs):
        return D.union_many(*input_dfs.values())

    return compute_function


transforms = (make_transform(groupName, datasetNames) for groupName, datasetNames in make_groups(configs.datasets))
