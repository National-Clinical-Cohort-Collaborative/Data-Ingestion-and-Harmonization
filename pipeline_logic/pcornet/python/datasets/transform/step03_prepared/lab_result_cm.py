from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from pcornet.utils import add_site_id_col, create_datetime_col
from pcornet.site_specific_utils import apply_site_parsing_logic
from pyspark.sql import functions as F


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/03 - prepared/lab_result_cm'),
    unmapped_covid_labs=Output(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/metadata/unmapped_covid_labs',
        # checks=Check(E.count().equals(0), 'Null lab_loinc value for raw_lab_name containing "SARS-CoV-2" or "COVID-19" -- (run testnorm_covid19 for LOINC mapping)', on_error='WARN')
        # Check moved to Data Health UI for issue creation/descrpition purposes
    ),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/02 - clean/lab_result_cm'),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 793'),
    covid_test_loinc_map=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/covid19_testnorm mapping/testnorm_covid19_mapping_table'),
)
def compute_function(my_input, site_id_df, covid_test_loinc_map, processed, unmapped_covid_labs):

    processed_df = my_input.dataframe()

    # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
    processed_df = add_site_id_col(processed_df, site_id_df)

    # Apply site-specific parsing logic (if applicable)
    processed_df = apply_site_parsing_logic(processed_df, site_id_df)

    processed_df = create_datetime_col(processed_df, "specimen_date", "specimen_time", "SPECIMEN_DATETIME")
    processed_df = create_datetime_col(processed_df, "result_date", "result_time", "RESULT_DATETIME")

    # Add LOINC codes from covid19_testnorm mapping table
    covid_test_df = covid_test_loinc_map.dataframe()
    processed_df = processed_df.join(
        covid_test_df,
        processed_df["raw_lab_name"] == covid_test_df["Covid19LabtestNames"],
        "left"
    )
    # Only use mapped codes for raw_lab_name entries containing "SARS-CoV-2" that don't already have LOINC code
    processed_df = processed_df.withColumn("lab_loinc", F.coalesce(processed_df["lab_loinc"], processed_df["AutoLoincCodes"]))

    # Flag any covid tests that did not have a mapping
    unmapped_tests_names = processed_df.where(
        (F.col("lab_loinc").isNull()) &
        (F.col("raw_lab_name").rlike("(?i).*sars-cov-2.*"))
        ).select("raw_lab_name").distinct()

    unmapped_covid_labs.write_dataframe(unmapped_tests_names)
    processed.write_dataframe(processed_df)
