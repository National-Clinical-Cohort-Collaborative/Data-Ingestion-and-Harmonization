from pyspark.sql.functions import col, lit, coalesce
from pyspark.sql import DataFrame
from pyspark.sql import types as T


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

    # System columns are uppercase
    df = (
        df
        .withColumn("COALESCED_MAPPED_CODE_SYSTEM", coalesce(col("mapped_code_system"), second_mapped_code_system_col))
        .withColumn("COALESCED_MAPPED_CODE", coalesce(col("mapped_code"), second_mapped_code_col))
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
    site_id = site_id_df.head().data_partner_id
    return site_id


# Create column to store an id for each site, to be used in SQL code for primary key generation
def add_site_id_col(df, site_id_df):
    site_id = get_site_id(site_id_df)
    df = df.withColumn("data_partner_id", lit(site_id).cast(T.IntegerType()))
    return df
