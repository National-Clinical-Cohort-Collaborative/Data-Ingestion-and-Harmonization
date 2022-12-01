from pyspark.sql.functions import col, when, lit
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Window


def apply_schema(df, schema_dict):

    # Ensure all columns are uppercase for casting
    for original_col_name in df.columns:
        df = df.withColumnRenamed(original_col_name, original_col_name.upper())

    # Cast columns to proper types
    input_cols = df.columns
    for col_name, col_type in schema_dict.items():
        if col_name in input_cols:
            if col_type == T.DateType():
                df = df.withColumn(col_name, F.coalesce(
                    df[col_name].cast(T.DateType()),                # Handle standard date format yyyy-MM-dd
                    F.to_date(df[col_name], format="dd-MMM-yy")     # Handle dates like 01-JAN-20
                ))
            else:
                df = df.withColumn(col_name, df[col_name].cast(col_type))
        else:
            # Populate column with null if it isn't in input table
            df = df.withColumn(col_name, lit(None).cast(col_type))

    # Rename columns to lowercase for cross-site unioning
    for original_col_name in df.columns:
        df = df.withColumnRenamed(original_col_name, original_col_name.lower())

    # Strip whitespace from string columns and replace empty strings with nulls
    for col_name, data_type in df.dtypes:
        if data_type == "string":
            df = df.withColumn(col_name, F.trim(df[col_name]))
            df = df.withColumn(col_name, F.when(df[col_name] != "", F.col(col_name)).otherwise(F.lit(None)))

    return df


# Parse single row df to fetch the integer site_id
def get_site_id(site_id_df):
    site_id_df = site_id_df.dataframe()
    site_id = site_id_df.head(1)[0].data_partner_id
    return site_id


# Create column to store an id for each site, to be used in SQL code for primary key generation
def add_site_id_col(df, site_id_df):
    site_id = get_site_id(site_id_df)
    df = df.withColumn("data_partner_id", lit(site_id))
    df = df.withColumn("data_partner_id", df["data_partner_id"].cast(T.IntegerType()))
    return df


# Create timestamp column from a DateType column and a string column containing time in HH:mm format
def create_datetime_col(df, date_col_name, time_col_name, timestamp_col_name):
    df = df.withColumn(timestamp_col_name, F.concat_ws(" ", df[date_col_name], df[time_col_name]))
    df = df.withColumn(timestamp_col_name, F.unix_timestamp(df[timestamp_col_name], format='yyyy-MM-dd HH:mm').cast('timestamp'))
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


def add_mapped_vocab_code_col(df, mapping_table, domain, source_vocab_col, mapped_col_name):
    mapping_table = mapping_table.where(F.col("cdm_domain") == domain).select("source_vocab_code", "mapped_vocab_code")
    new_df = df.join(
        mapping_table,
        df[source_vocab_col] == mapping_table["source_vocab_code"],
        "left"
    ).withColumnRenamed(
        "mapped_vocab_code", mapped_col_name
    ).drop("source_vocab_code")
    return new_df