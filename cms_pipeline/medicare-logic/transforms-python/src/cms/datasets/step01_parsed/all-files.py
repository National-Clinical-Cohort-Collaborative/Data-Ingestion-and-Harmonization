from transforms.api import transform, Input, Output, Check, configure
from transforms import expectations as E
from source_cdm_utils.parse import parse_csv
from cms import configs


def make_transform(dataset):
    @configure(profile=['DRIVER_MEMORY_EXTRA_EXTRA_LARGE', 'EXECUTOR_MEMORY_OVERHEAD_EXTRA_LARGE'])
    @transform(
        csvs=Input(configs.transform + "00 - unzipped/unzipped_raw_data"),
        processed_output=Output(configs.transform + "01 - parsed/processed/" + dataset.name
        ),
        error_output=Output(configs.transform + "01 - parsed/errors/" + dataset.name)
    )
    def compute_function(csvs, processed_output, error_output):
        processed_df, error_df = parse_csv(csvs, dataset.name, dataset.column_list, dataset.column_list)
        processed_output.write_dataframe(processed_df)
        error_output.write_dataframe(error_df)

    return compute_function


transforms = (make_transform(dataset) for dataset in configs.datasets)
