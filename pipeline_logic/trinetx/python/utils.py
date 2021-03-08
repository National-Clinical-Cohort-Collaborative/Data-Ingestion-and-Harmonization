from pyspark.sql.functions import col, when, lit, coalesce
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Window


# Replace empty strings with null values 
def blanks_as_nulls(df: DataFrame) -> DataFrame:
    def blank_as_null(x):
        return when(col(x) == "", None).otherwise(col(x))
    exprs = [blank_as_null(x).alias(x) for x in df.columns]
    return df.select(*exprs)


# Several TriNetX domain tables have MAPPED_CODE_SYSTEM and MAPPED_CODE columns which may be null
# If they are null, we fall back on using a secondary pair of _CODE_SYSTEM and _CODE columns to perform joins
def add_coalesced_code_cols(df, second_mapped_code_system_col=None, second_mapped_code_col=None):
    if second_mapped_code_system_col is None:
        second_mapped_code_system_col = lit(None)
    else:
        second_mapped_code_system_col = col(second_mapped_code_system_col)

    if second_mapped_code_col is None:
        second_mapped_code_col = lit(None)
    else:
        second_mapped_code_col = col(second_mapped_code_col)

    df = (
        df
        .withColumn("COALESCED_MAPPED_CODE_SYSTEM", coalesce(col("MAPPED_CODE_SYSTEM"), second_mapped_code_system_col))
        .withColumn("COALESCED_MAPPED_CODE", coalesce(col("MAPPED_CODE"), second_mapped_code_col))
    )
    return df


# Use TriNetX Value Mapping Table to add columns that will be used for joins on concept table
# Maps columns that might have formatting issues or otherwise do not match the concept table to new values
def add_mapping_cols(
        df: DataFrame,
        mapping_df: DataFrame,
        site_id_df: DataFrame,
        domain,
        columns
        ):

    site_id = str(get_site_id(site_id_df))

    for column in columns:
        conditions = [
            (mapping_df["site"] == site_id) | (mapping_df["site"] == "ALL"),
            mapping_df["trinetx_domain"] == domain,
            mapping_df["trinetx_domain_column"] == column,
            df[column] == mapping_df["source_value"]
        ]
        df = df.alias("a").join(mapping_df.alias("b"), conditions, how='left')\
            .selectExpr("a.*", "coalesce(b.mapped_value, a.{col_name}) as PREPARED_{col_name}".format(col_name=column))

    return df


# Parse single row df to fetch the integer site_id
def get_site_id(site_id_df):
    site_id_df = site_id_df.dataframe()
    site_id = site_id_df.head(1)[0].data_partner_id
    return site_id


# Create column to store an id for each site, to be used in SQL code for primary key generation
def add_site_id_col(df, site_id_df):
    site_id = get_site_id(site_id_df)
    df = df.withColumn("DATA_PARTNER_ID", lit(site_id))
    df = df.withColumn("DATA_PARTNER_ID", df["DATA_PARTNER_ID"].cast(T.IntegerType()))
    return df


# Given a filesystem dataframe containing payload zip files, return the most recent payload name
def get_newest_payload(files_df):
    # Look for 8 digits at the end of the filepath representing payload date
    files_df = files_df.withColumn("payload_date", F.regexp_extract(F.col("path"), "(?i)(\d{8})(.*)(\.zip)$", 1))
    # Handle either yyyyMMDD or MMDDyyyy format
    files_df = files_df.withColumn(
        "processed_date",
        F.when(F.regexp_extract(F.col("path"), "(202.)\d{4}", 1) == F.lit(""), F.concat(F.col("payload_date").substr(5,4), F.col("payload_date").substr(1,4)))\
        .otherwise(F.col("payload_date"))
    )
    # If site submitted multiple files on the same day (e.g. "payload_20201015.zip" and "payload_20201015_1.zip", extract the increment
    files_df = files_df.withColumn("same_date_increment", F.regexp_extract(F.col("path"), "(?i)(\d{8})(.*)(\.zip)$", 2))

    # Sort by processed payload date, then by increment, then by modified time and grab the most recent payload
    files_df = files_df.orderBy(["processed_date", "same_date_increment", "modified"], ascending=False)
    newest_file = files_df.take(1)[0].path

    return newest_file
