from transforms.api import transform_df, Input, Output
from myproject.anchor import path
from source_cdm_utils import status


@transform_df(
    Output(path.metadata + "payload_status"),
    start_df=Input(path.transform + "00 - unzipped/payload_filename"),
    end_df=Input(path.union_staging + "staged/" + "person"),
    site_id=Input(path.site_id)
)
def my_compute_function(ctx, start_df, end_df, site_id):
    return status.payload_status(ctx, start_df, end_df, site_id)
