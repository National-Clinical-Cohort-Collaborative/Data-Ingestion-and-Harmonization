from transforms.api import transform, Input, Output, Markings
from source_cdm_utils.unzip import extract_filenames
from cms import configs


@transform(
    zip_file=Input(configs.zipInput,
                            stop_propagating=Markings(["032e4f9b-276a-48dc-9fb8-0557136ffe47"],
                            on_branches=["master"])),
    payload_filename=Output(configs.transform + "00 - unzipped/payload_filename")
)
def extract(ctx, payload_filename, zip_file):
    regex = "(?i)Medicare_.*\\.zip"
    extract_filenames(ctx, zip_file, regex, payload_filename)
