# from pyspark.sql import functions as F
from transforms.api import transform, Input, Output
from source_cdm_utils.parse import parse_csv

file_list = ['pecos_ccn.csv',
'pecos_npi.csv']


def make_transform(dataset):
    @transform(
        processed_output=Output('/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/' + dataset),
        error_output=Output('/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/errors_' + dataset),
        csvs=Input("ri.foundry.main.dataset.ca97439c-ca3c-4cf2-bbe7-cad2a994e774")

    )
    def compute_function(csvs, processed_output, error_output):
        processed_df, error_df = parse_csv(csvs, dataset, [], [])
        processed_output.write_dataframe(processed_df)
        error_output.write_dataframe(error_df)

    return compute_function


transforms = (make_transform(filename) for filename in file_list)
