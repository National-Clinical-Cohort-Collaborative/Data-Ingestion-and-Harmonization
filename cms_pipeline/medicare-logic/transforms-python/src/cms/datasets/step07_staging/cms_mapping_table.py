from pyspark.sql import functions as F
from transforms.api import transform, Input, Output
from cms.utils import fixedSizeHash


@transform(
    output=Output("ri.foundry.main.dataset.8f3bc290-9ec9-4a49-b28b-3b4d8d34298d"),
    pepper=Input("ri.foundry.main.dataset.be49593a-56ce-4683-b126-ff667fe05be3"),
    opt_in=Input("ri.foundry.main.dataset.e0b86897-c6f0-4317-938d-2840fe0e2117")
    )
def compute(pepper, opt_in, output):
    opt_in = opt_in.dataframe()
    pepper = pepper.dataframe()
    pepper_person = pepper.filter(pepper.Variable == "pepper_person").collect()[0].UUID  # noqa

    person_col = "cms_person_id"
    opt_in = opt_in.withColumn("original_cms_person_id", opt_in.cms_person_id)
    opt_in = opt_in.withColumn(person_col, fixedSizeHash(F.col(person_col), pepper_person, 59).cast("string"))
    opt_in = opt_in.withColumn(person_col, F.concat(F.lit("MEDICARE"), opt_in[person_col]))
    opt_in = opt_in.select('cms_person_id', 'n3c_person_id', 'original_cms_person_id', 'data_partner_id')
    opt_in = opt_in.withColumn('data_partner_id', opt_in.data_partner_id.cast('integer'))
    output.write_dataframe(opt_in)
