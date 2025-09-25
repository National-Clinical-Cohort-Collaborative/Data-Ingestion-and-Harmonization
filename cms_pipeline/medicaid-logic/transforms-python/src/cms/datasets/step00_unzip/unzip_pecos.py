# from pyspark.sql import functions as F
from transforms.api import transform, Input, Output
from source_cdm_utils.unzip import unzipLatest


@transform(
    pecos_unzipped=Output("ri.foundry.main.dataset.ca97439c-ca3c-4cf2-bbe7-cad2a994e774"),
    pecos_zip=Input("ri.foundry.main.dataset.6baac674-a648-4dd3-8cdb-46990ed2d911"),
)
def compute(pecos_zip, pecos_unzipped):
    pecos_regex = "pecos.*\\.zip"
    unzipLatest(pecos_zip, pecos_regex, pecos_unzipped)