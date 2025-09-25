from transforms.api import transform_df, Input, Output
from transforms.verbs import dataframes as D
from cms import configs

# Get all group names
input_names = []
for dataset in configs.datasets:
    for groupName in dataset.melting:
        input_names.append(dataset.name + "_" + groupName)

# Build into inputs
input_dfs = {name: Input(configs.transform + "03 - melted/initial/" + name) for name in input_names}


@transform_df(
    Output(configs.transform + "03 - melted/all"),
    **input_dfs
)
def compute_function(**input_dfs):
    return D.union_many(*input_dfs.values()).select(configs.melted_columns)
