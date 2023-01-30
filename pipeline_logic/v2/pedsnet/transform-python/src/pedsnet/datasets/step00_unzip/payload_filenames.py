from transforms.api import transform, Input, Output
from source_cdm_utils.unzip import extract_filenames
from pedsnet.anchor import path


@transform(
    zip_file=Input(path.input_zip),
    payload_filename=Output(path.transform + "00 - unzipped/payload_filename")
)
def extract(ctx, payload_filename, zip_file):
    regex = "(?i).*incoming/.*_OMOP_.*\\.zip"  # Note that the "_OMOP_" is intentional
    extract_filenames(ctx, zip_file, regex, payload_filename)
