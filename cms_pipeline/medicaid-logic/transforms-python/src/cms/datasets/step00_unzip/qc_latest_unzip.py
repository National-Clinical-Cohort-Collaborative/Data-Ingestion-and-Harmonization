from transforms.api import transform, Input, Output, Markings, Check
from source_cdm_utils.unzip import verify_newest_payload_is_latest
from cms import configs
from transforms import expectations as E

# We assume the check is valid if the zip file containing the latest date is unzipped
expect_valid_latest_file = E.all(
    E.col('newest_payload').rlike(r"^20."),
    E.col('latest_date_check_passed').equals(True)
)


@transform(
    zip_file=Input(configs.zipMDCDInput,
                            stop_propagating=Markings(["032e4f9b-276a-48dc-9fb8-0557136ffe47"],
                            on_branches=["master"])),
    qc_filename=Output(configs.transform + "00 - unzipped/qc_latest_filename",
        checks=Check(expect_valid_latest_file, 'Valid latest file in output', on_error='FAIL')
    )
)
def extract(ctx, qc_filename, zip_file):
    regex = "(?i)Medicaid_.*\\.zip"
    verify_newest_payload_is_latest(ctx, zip_file, regex, qc_filename)