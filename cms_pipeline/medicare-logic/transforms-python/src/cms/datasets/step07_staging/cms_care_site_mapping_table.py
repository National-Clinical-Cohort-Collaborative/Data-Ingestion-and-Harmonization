# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


# input_folder = "/UNITE/[PPRL] CMS Data & Repository/pipeline/transform/06 - schema check"
# output folder should be in the release folder : "/UNITE/[PPRL] CMS Data & Repository/pipeline/staging/cms_care_site_mapping_table"),
@transform_df(
    Output("/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/release_restricted/cms_care_site_mapping_table"),
    step5_care_site=Input("ri.foundry.main.dataset.513c06ff-90a0-49b9-b3f6-d230cb0cccc3")
)
def compute(step5_care_site):
    # join original care_site_id from step 4 with masked care_site_id created from pepper in the step_5
    return step5_care_site.select("care_site_id", "orig_care_site_id",  "data_partner_id")
