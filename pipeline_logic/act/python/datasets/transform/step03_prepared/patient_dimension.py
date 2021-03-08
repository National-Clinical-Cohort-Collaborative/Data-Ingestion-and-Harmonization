from transforms.api import transform, Input, Output
from act.utils import add_site_id_col
from act.site_specific_utils import apply_site_parsing_logic
from pyspark.sql import functions as F
from pyspark.sql import types as T


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/patient_dimension'),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/02 - clean/patient_dimension'),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 411'),
)
def compute_function(my_input, site_id_df, processed):

    processed_df = my_input.dataframe()

    # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    # Add ethnicity_cd column if it is not present (optional ACT column)
    if "ethnicity_cd" not in processed_df.columns:
        processed_df = processed_df.withColumn("ethnicity_cd", F.lit(None).cast(T.StringType()))

    processed.write_dataframe(processed_df)
