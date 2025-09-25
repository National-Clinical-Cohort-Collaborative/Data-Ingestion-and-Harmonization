from pyspark.sql import functions as F
from transforms.api import transform, Input, Output
from source_cdm_utils.parse import parse_csv
from cms import configs


def make_transform(dataset):
    @transform(
        csvs=Input('ri.foundry.main.dataset.01f94c1b-3f84-4d24-b346-30ee305e86f7'),
        processed_output=Output(configs.parsed_path + dataset),
        error_output=Output(configs.parsed_path + "errors/errors_" + dataset)
    )
    def compute_function(csvs, processed_output, error_output):
        processed_df, error_df = parse_csv(csvs, dataset, [], [])
        processed_df = processed_df.dropDuplicates()
        YEAR = str(dataset[-8:-4])
        processed_df = processed_df.withColumn("YEAR", F.lit(YEAR))
        processed_output.write_dataframe(processed_df)
        error_output.write_dataframe(error_df)

    return compute_function


transforms = (make_transform(filename) for filename in configs.input_file_list)
