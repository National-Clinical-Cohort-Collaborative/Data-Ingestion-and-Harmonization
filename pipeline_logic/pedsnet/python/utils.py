from pyspark.sql.functions import lit, col, when
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from myproject.schemas import required_domain_schema_dict, null_cols_to_drop_dict

primary_key_cols = [
    'CARE_SITE_ID',
    'CONDITION_ERA_ID',
    'CONDITION_OCCURRENCE_ID',
    'DOSE_ERA_ID',
    'DRUG_ERA_ID',
    'DRUG_EXPOSURE_ID',
    'LOCATION_ID',
    'MEASUREMENT_ID',
    'OBSERVATION_ID',
    'OBSERVATION_PERIOD_ID',
    'PERSON_ID',
    'PROCEDURE_OCCURRENCE_ID',
    'PROVIDER_ID',
    'VISIT_OCCURRENCE_ID',
]


def apply_schema(df, schema_dict):

    # Convert empty strings to null values
    def blank_as_null(x):
        return when(col(x) != "", col(x)).otherwise(None)
    exprs = [blank_as_null(x).alias(x) for x in df.columns]
    df = df.select(*exprs)

    # Make sure all column names are uppercase
    for original_col_name in df.columns:
        df = df.withColumnRenamed(original_col_name, original_col_name.upper())
    input_cols = df.columns

    # Iterate through all columns in OMOP domain schema
    # Cast those that are present to proper types, or create empty columns
    for col_name, col_type in schema_dict.items():
        if col_name in input_cols:
            if col_name in primary_key_cols:
                # Cast primary keys to Decimal type before converting to ultimate type
                # to handle issues with scientific notation
                df = df.withColumn(col_name, df[col_name].cast(T.DecimalType(20, 0)))

            elif col_type == T.DateType() or col_type == T.TimestampType():
                # Handle dates/datetimes in Unix timestamp format or in regular string timestamp format
                df = df.withColumn(col_name, F.coalesce(F.from_unixtime(df[col_name]), df[col_name]))

            df = df.withColumn(col_name, df[col_name].cast(col_type))

        else:
            # Populate column with null if it isn't in input table
            df = df.withColumn(col_name, lit(None).cast(col_type))

    # Rename columns to lowercase for cross-site unioning
    for original_col_name in df.columns:
        df = df.withColumnRenamed(original_col_name, original_col_name.lower())

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


def new_duplicate_rows_with_collision_bits(omop_domain, lookup_df, ctx, pk_col, full_hash_col):

    # Extract all duplicate rows from domain table
    # Keep two columns: 51 bit hash (which caused collision) and full hash (to differentiate collisions)
    w = Window.partitionBy(pk_col)
    duplicates_df = omop_domain.dataframe().select('*', F.count(pk_col).over(w).alias('dupeCount'))\
        .where('dupeCount > 1')\
        .drop('dupeCount')
    duplicates_df = duplicates_df.select(pk_col, full_hash_col)

    if ctx.is_incremental:
        # Count how many rows in the lookup table exist for the collided hash value
        cache = lookup_df.dataframe('previous', schema=T.StructType([
            T.StructField(pk_col, T.LongType(), True),
            T.StructField(full_hash_col, T.StringType(), True),
            T.StructField("collision_bits", T.IntegerType(), True)
        ]))
        cache_count = cache.groupby(pk_col).count()

        # Keep only the rows in duplicates_df that are not currently in lookup table
        cond = [pk_col, full_hash_col]
        duplicates_df = duplicates_df.join(cache, cond, 'left_anti')

    # Create counter for rows in duplicates_df
    # Subtract 1 because the default collision resolution bit value is 0
    w2 = Window.partitionBy(pk_col).orderBy(pk_col)
    duplicates_df = duplicates_df.withColumn('row_num', F.row_number().over(w2))
    duplicates_df = duplicates_df.withColumn('row_num', (F.col('row_num') - 1))

    # If there are already entries in the lookup table for the given primary key,
    # then add the number of existing entries to the row number counter
    if ctx.is_incremental:
        duplicates_df = duplicates_df.join(cache_count, pk_col, 'left')
        duplicates_df = duplicates_df.fillna(0, subset=['count'])
        duplicates_df = duplicates_df.withColumn('row_num', (F.col('row_num') + F.col('count').cast(T.IntegerType())))

    duplicates_df = duplicates_df.withColumnRenamed('row_num', 'collision_bits')

    # Remove 'count' column for incremental transforms: 
    duplicates_df = duplicates_df.select(pk_col, full_hash_col, 'collision_bits')

    return duplicates_df


# Remove all rows containing null values for certain required OMOP fields
# Only drop nulls from columns deemed 100% essential
def drop_nulls_from_selected_required_cols(df, domain):
    required_cols = [col.lower() for col in null_cols_to_drop_dict[domain].keys()]
    for col_name in required_cols:
        df = df.filter(F.col(col_name).isNotNull())

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


# Map source concept_ids in concept_col to standard concepts
# Use concept and concept_relationship tables to fill out source/target concept_id info
def perform_standard_mapping(df, concept_df, concept_relationship_df, concept_col):
    original_cols = df.columns

    # Map source concept_ids to standard concept_ids when possible
    # Note that standard concept_ids map to themselves in the concept_relationship table
    df = df.join(
        concept_relationship_df,
        on=(
            (df[concept_col] == concept_relationship_df["concept_id_1"]) &
            (F.lower(concept_relationship_df["relationship_id"]) == "maps to") &
            (concept_relationship_df["invalid_reason"].isNull())
        ),
        how="left"
    ).withColumn("source_concept_id", df[concept_col])\
     .withColumnRenamed("concept_id_2", "target_concept_id")

    # Bring in relevant concept info for source concepts
    df = df.join(
        concept_df,
        (df["source_concept_id"] == concept_df["concept_id"]),
        "left"
    )
    df = df.selectExpr(
        *original_cols,
        "source_concept_id",
        "target_concept_id",
        "concept_name as source_concept_name",
        "domain_id as source_domain_id",
        "vocabulary_id as source_vocabulary_id",
        "concept_class_id as source_concept_class_id",
        "standard_concept as source_standard_concept"
    )
    cols_to_keep = df.columns

    # Bring in relevant concept info for target concepts
    df = df.join(
        concept_df,
        (df["target_concept_id"] == concept_df["concept_id"]),
        "left"
    )
    df = df.selectExpr(
        *cols_to_keep,
        "concept_name as target_concept_name",
        "domain_id as target_domain_id",
        "vocabulary_id as target_vocabulary_id",
        "concept_class_id as target_concept_class_id",
        "standard_concept as target_standard_concept"
    )

    return df
