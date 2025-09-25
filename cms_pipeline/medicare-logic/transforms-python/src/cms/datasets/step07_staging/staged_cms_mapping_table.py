# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[PPRL] CMS Data & Repository/pipeline/staging/staged_cms_mapping_table"),
    cms_mapping_table=Input("ri.foundry.main.dataset.8f3bc290-9ec9-4a49-b28b-3b4d8d34298d"),
)
def compute(cms_mapping_table):
    cms_mapping_table = cms_mapping_table.drop('original_cms_person_id')
    return cms_mapping_table
