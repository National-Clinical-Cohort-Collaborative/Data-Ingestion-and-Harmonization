from transforms.api import transform, Input, Output, incremental, Check
from transforms import expectations as E
from pcornet.pkey_utils import new_duplicate_rows_with_collision_bits


@incremental(snapshot_inputs=['omop_domain'])
@transform(
    lookup_df=Output(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/05 - pkey collision lookup tables/observation",
        checks=Check(E.col('collision_bits').lt(4), 'Fewer than 3 collisions for each 51-bit id', on_error='FAIL')
    ),
    omop_domain=Input(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/04 - domain mapping/observation",
        checks=Check(E.primary_key('hashed_id'), 'hashed_id is unique', on_error='FAIL')
    ),
)
def my_compute_function(omop_domain, lookup_df, ctx):

    pk_col = "observation_id_51_bit"
    full_hash_col = "hashed_id"

    new_rows = new_duplicate_rows_with_collision_bits(omop_domain, lookup_df, ctx, pk_col, full_hash_col)
    lookup_df.write_dataframe(new_rows)
