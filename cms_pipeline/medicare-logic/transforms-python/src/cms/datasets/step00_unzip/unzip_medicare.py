from transforms.api import transform, Input, Output, Markings
from source_cdm_utils.unzip import unzipLatest
from cms import configs


@transform(
    foundryInput=Input(configs.zipInput,
                            stop_propagating=Markings(["032e4f9b-276a-48dc-9fb8-0557136ffe47"],
                            on_branches=["master"])),
    foundryOutput=Output(configs.transform + "00 - unzipped/unzipped_raw_data"),
)
def compute(foundryInput, foundryOutput):

    data_regex = "(?i)Medicare_.*\\.zip"
    unzipLatest(foundryInput, data_regex, foundryOutput)
