from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import configs


@transform_df(
    Output(configs.metadata + "person_map"),
    pprl=Input("ri.foundry.main.dataset.4d6aec85-b80a-4228-9436-9a5271a856bf"),
    institutions=Input("ri.foundry.main.dataset.4d4cf17b-9dfb-48e8-bb19-4f62960b75ec"),
    person_ids=Input("ri.foundry.main.dataset.bfe18a80-5940-467f-85ad-9d9c5a95f5e2")
)
def compute(pprl, institutions, person_ids):
    # Get CMS vs. everyone else
    cms = pprl.filter(F.col("institution") == "CMS-MEDICAID").alias("cms")
    others = pprl.filter((F.col("institution") != "CMS-MEDICAID") & (F.col("institution") != "CMS")).alias("others")
    # Join CMS to everyone else to get the main mapping
    cms_groups = cms.join(others, "group")
    cms_groups = cms_groups.select(
        F.col("cms.site_person_id").alias("cms_person_id"),
        F.col("others.institution").alias("institution"),
        F.col("others.site_person_id").alias("site_person_id")
    )

    # Convert institution to data_partner_id
    cms_groups = cms_groups.withColumn("lhb_id", F.concat_ws(':', cms_groups.institution, cms_groups.site_person_id))
    institutions = institutions.select("abbreviation", "data_partner_id")
    # Capitalize all site abbreviations in order to match correctly
    institutions = institutions.withColumn("institution", F.upper(F.col("abbreviation"))).drop("abbreviation")
    # Remove JHU testing row in Data Partner IDs
    institutions = institutions.filter(F.col("site_name") != "TESTING - DO NOT RELEASE jhu omop")
    institutions = institutions.dropDuplicates()
    with_data_partners = cms_groups.join(institutions, "institution").drop("institution")

    # Get the N3C person_id
    with_n3c_ids = with_data_partners.join(person_ids, ["data_partner_id", "site_person_id"])

    # Return
    return with_n3c_ids.select("cms_person_id", "data_partner_id", "n3c_person_id", "lhb_id")
