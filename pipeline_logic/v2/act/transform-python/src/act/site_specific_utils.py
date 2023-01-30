# Code that should only be deployed for particular sites
# (e.g. parsing based on site-specific data issues)

from act.utils import get_site_id
from pyspark.sql import functions as F


def strip_extra_zero(df):
    if "patient_num" in df.columns:
        df = df.withColumn("patient_num", F.regexp_replace(df["patient_num"], "\.0$", ""))
    if "encounter_num" in df.columns:
        df = df.withColumn("encounter_num", F.regexp_replace(df["encounter_num"], "\.0$", ""))
    return df


def remove_duplicate_rows(df):
    metadata_cols = ["update_date", "download_date", "import_date", "sourcesystem_cd", "upload_id"]
    non_metadata_cols = [col for col in df.columns if col not in metadata_cols]
    df = df.select(non_metadata_cols).distinct()
    return df


site_function_dict = {
    224: [strip_extra_zero, remove_duplicate_rows]
}


def apply_site_parsing_logic(df, site_id_df):
    site_id = get_site_id(site_id_df)
    if site_id in site_function_dict:
        for func in site_function_dict[site_id]:
            df = func(df)
        return df
    else:
        return df


# dialect parameters based on csv module: https://docs.python.org/3/library/csv.html#dialects-and-formatting-parameters
# these override whatever the csv.Sniffer detects for the csv file
site_csv_dialect_dict = {
    (1003, "visit_dimension"): {"escapechar": None},
    (1003, "observation_fact"): {"escapechar": None, "doublequote": True, "quotechar": '"'}
}


def get_site_dialect_params(site_id, domain):
    # Default to using backslash as escapechar
    return site_csv_dialect_dict.get((site_id, domain), {})
