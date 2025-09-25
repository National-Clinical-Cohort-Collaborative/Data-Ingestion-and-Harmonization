from transforms.api import transform, Input, Output
from cms import configs

@transform(
    gpi_table=Input("ri.foundry.main.dataset.f1b19348-81b6-4b0f-81bf-81a724fd482f"),
    processed_output=Output(configs.transform + "01 - parsed/processed/pprl_mapping")
)
def compute_function(gpi_table, processed_output):
    gpi = gpi_table.dataframe()
    processed_output.write_dataframe(gpi)
