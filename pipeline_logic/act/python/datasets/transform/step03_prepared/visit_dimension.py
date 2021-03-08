from transforms.api import transform, Input, Output
from act.utils import add_site_id_col
from act.site_specific_utils import apply_site_parsing_logic
from pyspark.sql.functions import col, concat_ws


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/03 - prepared/visit_dimension'),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/02 - clean/visit_dimension'),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 411'),
)
def compute_function(my_input, site_id_df, processed):

    processed_df = my_input.dataframe()

    # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    # Create new primary key column since some sites send data with duplicate encounter_num entries
    processed_df = processed_df.withColumn("VISIT_DIMENSION_ID", concat_ws("|", col("encounter_num"), col("patient_num")))

    processed.write_dataframe(processed_df)
