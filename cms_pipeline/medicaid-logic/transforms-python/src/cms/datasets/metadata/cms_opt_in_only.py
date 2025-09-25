from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output, Markings
from cms.utils import FINAL_OPT_IN_COLS
from cms import configs


@transform_df(
    Output(configs.metadata + "cms_opt_in_only"),
    cms_all=Input(configs.metadata + "person_map"),
    data_partner_id_map=Input("ri.foundry.main.dataset.4d4cf17b-9dfb-48e8-bb19-4f62960b75ec"),
    pprl_participation=Input("ri.foundry.main.dataset.1dd5c179-f0f0-4bd3-8f26-7469c94fed2a",
                            stop_propagating=Markings(
                                ["5e003460-bb7d-4553-9049-922ad376eb23"],
                            on_branches=["master"])
                            )
)
def compute(cms_all, data_partner_id_map, pprl_participation):

    # Get sites who have opted-in to PPRL data linkage
    pprl_participation = pprl_participation \
        .filter(F.col("feature_name") == "CMS") \
        .filter(F.col("is_participating_yn") == "Yes")

    # Get institution ids
    data_partner_id_map = data_partner_id_map.select("data_partner_id", "institutionid").dropDuplicates()

    cms_all = cms_all \
        .join(data_partner_id_map,
              on="data_partner_id",
              how="inner")

    # Filter to only sites that have opted-in to PPRL data linkage
    cms_filtered = cms_all \
        .join(pprl_participation.select("institutionid"),
              on="institutionid",
              how="inner")

    return cms_filtered.select(*FINAL_OPT_IN_COLS)
