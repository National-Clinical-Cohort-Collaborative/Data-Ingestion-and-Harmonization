# Code that should only be deployed for particular sites
# (e.g. parsing based on site-specific data issues)

from trinetx.utils import get_site_id
from pyspark.sql import functions as F


def site_353_parsing(df):
    if 'COALESCED_MAPPED_CODE' in df.columns:
        df = df.withColumn(
            'COALESCED_MAPPED_CODE',
            F.regexp_replace(F.col('COALESCED_MAPPED_CODE'), "UNMC-", "")
        )
    return df


site_function_dict = {
    353: site_353_parsing
}


def apply_site_parsing_logic(df, site_id_df):
    site_id = get_site_id(site_id_df)
    if site_id in site_function_dict:
        return site_function_dict[site_id](df)
    else:
        return df


# dialect parameters based on csv module: https://docs.python.org/3/library/csv.html#dialects-and-formatting-parameters
# these override whatever the csv.Sniffer detects for the csv file
site_csv_dialect_dict = {
}


def get_site_dialect_params(site_id, domain):
    # Default to using backslash as escapechar
    return site_csv_dialect_dict.get((site_id, domain), {})
