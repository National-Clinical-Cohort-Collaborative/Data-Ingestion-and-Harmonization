from transforms.api import transform, Input, Output, incremental, Check
from transforms import expectations as E
from source_cdm_utils import collision_check
from myproject.anchor import path

input_check = Check(E.primary_key('hashed_id'), 'hashed_id is unique', on_error='FAIL')
output_check = Check(E.col('collision_bits').lt(4), 'Fewer than 3 collisions for each 51-bit id', on_error='FAIL')


def make_transform(domain, pkey):
    @incremental(snapshot_inputs=['omop_domain'])
    @transform(
        omop_domain=Input(path.transform + "03 - local id generation/" + domain, checks=input_check),
        lookup_df=Output(path.transform + "04 - id collision lookup tables/" + domain, checks=output_check),
    )
    def compute_function(ctx, omop_domain, lookup_df):
        new_rows = collision_check.new_duplicate_rows_with_collision_bits(omop_domain, lookup_df, ctx, pkey, "hashed_id")
        lookup_df.write_dataframe(new_rows)

    return compute_function


domains = [
    ("care_site", "care_site_id_51_bit"),
    ("condition_era", "condition_era_id_51_bit"),
    ("condition_occurrence", "condition_occurrence_id_51_bit"),
    # Deaths do not have primary keys
    ("device_exposure", "device_exposure_id_51_bit"),
    ("dose_era", "dose_era_id_51_bit"),
    ("drug_era", "drug_era_id_51_bit"),
    ("drug_exposure", "drug_exposure_id_51_bit"),
    ("location", "location_id_51_bit"),
    ("measurement", "measurement_id_51_bit"),
    ("note", "note_id_51_bit"),
    ("note_nlp", "note_nlp_id_51_bit"),
    ("observation", "observation_id_51_bit"),
    ("observation_period", "observation_period_id_51_bit"),
    ("person", "person_id_51_bit"),
    ("procedure_occurrence", "procedure_occurrence_id_51_bit"),
    ("provider", "provider_id_51_bit"),
    ("visit_detail", "visit_detail_id_51_bit"),
    ("visit_occurrence", "visit_occurrence_id_51_bit"),
    ("control_map", "control_map_id_51_bit"),
]

transforms = (make_transform(*domain) for domain in domains)
