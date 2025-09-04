# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F


@transform_df(
    Output("/UNITE/Unit Harmonization/Maintainer Resources for Unit Harmonization/datasets/UHI_codeset_members"),
    canonical_units_df=Input("/UNITE/Unit Harmonization/canonical_units_of_measure"),
    concept_set_members=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
)
def compute(canonical_units_df, concept_set_members):
    uhi_concept_set_members = canonical_units_df.join(concept_set_members, 'codeset_id', 'inner') \
                                                .select('codeset_id', 'measured_variable', 'concept_id',
                                                        'concept_set_name', 'is_most_recent_version', 'version',
                                                        'concept_name', 'archived').distinct()
    return uhi_concept_set_members.sort(F.col("measured_variable"), F.col("concept_name"))
