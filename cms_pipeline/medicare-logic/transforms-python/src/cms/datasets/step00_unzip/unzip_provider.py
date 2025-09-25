from transforms.api import transform, Input, Output, Markings
from source_cdm_utils.unzip import unzipLatest
from cms import configs


@transform(
    foundryProviderInput=Input(configs.zipProviderInput),
    foundryProviderOutput=Output(configs.provider + "00 - unzipped/unzipped_raw_data")
)
def compute(foundryProviderInput, foundryProviderOutput):
    provider_regex = "(?i).*_Provider_Files_to_N3C\\.zip"
    unzipLatest(foundryProviderInput, provider_regex, foundryProviderOutput)
