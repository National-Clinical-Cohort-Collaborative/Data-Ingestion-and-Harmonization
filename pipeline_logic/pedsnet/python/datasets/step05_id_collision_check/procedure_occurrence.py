from transforms.api import transform, Input, Output, incremental, Check
from transforms import expectations as E
from myproject.utils import new_duplicate_rows_with_collision_bits


@incremental(snapshot_inputs=['omop_domain'])
@transform(
    lookup_df=Output(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/05 - id collision lookup tables/procedure_occurrence",
        checks=Check(E.col('collision_bits').lt(4), 'Fewer than 3 collisions for each 51-bit id', on_error='FAIL')
    ),
    omop_domain=Input(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/04 - mapping/procedure_occurrence",
        checks=Check(E.primary_key('hashed_id'), 'hashed_id is unique', on_error='FAIL')
    ),
)
def my_compute_function(omop_domain, lookup_df, ctx):

    pk_col = "procedure_occurrence_id_51_bit"
    full_hash_col = "hashed_id"

    new_rows = new_duplicate_rows_with_collision_bits(omop_domain, lookup_df, ctx, pk_col, full_hash_col)
    lookup_df.write_dataframe(new_rows)
