# from pyspark.sql import functions as F
from transforms.api import transform, Input, Output
from source_cdm_utils.parse import parse_csv
# from cms import configs

file_list = ['2018-group-practice-linkage-public-file.csv',
# '2022_10_21_N3C_Prov_Report.xlsx',
'chsp-compendium-2018-updated-2021.csv',
'chsp-hospital-linkage-2018.csv',
'full_pos_2017_2022.csv',
'nppes_puf_20220925.csv',
'pec_enrlmt_npi.csv',
'pec_enrlmt_tin_adr.csv',
'pec_enrlmt_tin_enh.csv']


def make_transform(dataset):
    @transform(
        csvs=Input('ri.foundry.main.dataset.e66a9009-b386-4178-840c-230a908aa6dc' ),
        processed_output=Output('/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/' + dataset),
        error_output=Output('/UNITE/[PPRL] CMS Data & Repository/analysis-provider-characterization/step01_parsed/errors_' + dataset)
    )
    def compute_function(csvs, processed_output, error_output):
        processed_df, error_df = parse_csv(csvs, dataset, [], [])
        processed_output.write_dataframe(processed_df)
        error_output.write_dataframe(error_df)

    return compute_function


transforms = (make_transform(filename) for filename in file_list)
