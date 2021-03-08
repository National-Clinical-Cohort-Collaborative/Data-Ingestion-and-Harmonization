from pyspark.sql.functions import col, when, lit
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import Window


def apply_schema(df, schema_dict):

    # Convert empty strings to null values
    def blank_as_null(x):
        return when(col(x) != "", col(x)).otherwise(None)
    exprs = [blank_as_null(x).alias(x) for x in df.columns]
    df = df.select(*exprs)

    # Make sure all column names are lowercase
    for original_col_name in df.columns:
        df = df.withColumnRenamed(original_col_name, original_col_name.lower())
    input_cols = df.columns

    # Cast columns to proper types
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


def split_concept_cd_col(df, parsed_vocab_col_name, parsed_concept_code_name):
    df_new = df.withColumn(
        parsed_vocab_col_name,
        F.when(F.col("mapped_concept_cd").isNotNull(), F.split(F.col("mapped_concept_cd"), ':').getItem(0))\
        .otherwise(F.split(F.col("concept_cd"), ':').getItem(0))
    ).withColumn(
        parsed_concept_code_name,
        F.when(F.col("mapped_concept_cd").isNotNull(), F.split(F.col("mapped_concept_cd"), ':').getItem(1))\
        .otherwise(F.split(F.col("concept_cd"), ':').getItem(1))
    ).withColumn(
        parsed_concept_code_name,
        F.when(F.col(parsed_concept_code_name).rlike(".*? (NEGATIVE|POSITIVE|PENDING|EQUIVOCAL)"), F.split(F.col(parsed_concept_code_name), ' ').getItem(0))\
        .otherwise(F.col(parsed_concept_code_name))
    )
    return df_new


def create_mapped_vocab_codes_col(df, vocab_mapping_table):
    # Strip the ":" character from the end of the local prefix
    vocab_mapping_table = vocab_mapping_table.select(
        "local_prefix", "omop_vocab"
    ).withColumn(
        "local_prefix", F.regexp_replace(F.col("local_prefix"), ':', '')
    )

    df_new = df.join(
        vocab_mapping_table,
        df["parsed_vocab_code"] == vocab_mapping_table["local_prefix"],
        "left"
    ).withColumn(
        "parsed_vocab_code_in_vocab_map", F.lit(F.col("omop_vocab").isNotNull())
    ).withColumn(
        "mapped_vocab_code", F.coalesce(F.col("omop_vocab"), F.col("parsed_vocab_code"))
    ).drop("local_prefix").drop("omop_vocab")
    return df_new
