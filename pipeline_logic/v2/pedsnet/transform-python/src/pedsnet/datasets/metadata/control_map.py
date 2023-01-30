from transforms.api import transform_df, Input, Output
from pedsnet.anchor import path


@transform_df(
    Output(path.metadata + "control_map"),
    control_map_input=Input(path.transform + "01 - parsed/optional/control_map"),
)
def my_compute_function(control_map_input):
    return control_map_input
