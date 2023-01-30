from transforms.api import transform, Input, Output
from source_cdm_utils.unzip import unzipLatest
from pedsnet.anchor import path


@transform(
    zip_file=Input(path.input_zip),
    unzipped=Output(path.transform + "00 - unzipped/unzipped_raw_data"),
)
def unzip(zip_file, unzipped):
    regex = "(?i).*incoming/.*_OMOP_.*\\.zip"  # Note that the "_OMOP_" is intentional
    unzipLatest(zip_file, regex, unzipped)
