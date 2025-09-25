from transforms.api import transform, Input, Output
from source_cdm_utils.unzip import unzipLatest
from cms import configs


@transform(
    foundryInput=Input(configs.zipMDCDInput),
    foundryTokenOutput=Output(configs.transform + "00 - unzipped/unzipped_raw_data"),
)
def compute(foundryInput, foundryTokenOutput):
    data_regex = "(?i)Medicaid_.*\\.zip"
    unzipLatest(foundryInput, data_regex, foundryTokenOutput)
