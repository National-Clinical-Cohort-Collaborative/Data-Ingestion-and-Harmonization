# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs

@transform_df(
    output=Output(configs.staging + "staged_cms_mapping_table"),
    cms_mapping_table=Input(configs.staging + "cms_mapping_table")
)
def compute(cms_mapping_table):
    cms_mapping_table = cms_mapping_table.drop('original_cms_person_id')
    return cms_mapping_table
