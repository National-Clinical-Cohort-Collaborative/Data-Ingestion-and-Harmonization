from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window as W


def new_duplicate_rows_with_collision_bits(omop_domain, lookup_df, ctx, pk_col, full_hash_col):

    # Extract all duplicate rows from domain table
    # Keep two columns: 51 bit hash (which caused collision) and full hash (to differentiate collisions)
    w = W.partitionBy(pk_col)
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
    w2 = W.partitionBy(pk_col).orderBy(pk_col)
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
