# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - prepared/dm_hcpcs"),
    source_df=Input("ri.foundry.main.dataset.4e0ddfef-97e3-4979-89f4-806d7cff09d0"),
    place_of_visit_xwalk=Input("/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/03 - xwalk/cms2omop_visit_xwalk")
)
def compute(source_df, place_of_visit_xwalk):

    selected_df = source_df.select('BID', 'CLAIM_ID', 'TAX_NUM', 'EXPNSDT1', 'EXPNSDT2', 'HCPCS_CD', 'PLCSRVC', 'TAX_NUM_ID')
    joined_df = selected_df.join(place_of_visit_xwalk, on=selected_df['PLCSRVC'] == place_of_visit_xwalk['source_concept_code'])
    return joined_df
