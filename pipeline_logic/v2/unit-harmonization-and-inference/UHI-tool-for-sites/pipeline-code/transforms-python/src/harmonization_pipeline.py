from transforms.api import configure
from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType
from unitConversions import conversionsDictionary


@configure(profile=['NUM_EXECUTORS_64', 'EXECUTOR_MEMORY_LARGE', 'EXECUTOR_CORES_EXTRA_LARGE', 'SHUFFLE_PARTITIONS_LARGE'])
@transform_df(
    Output("ri.foundry.main.dataset.dde61c85-468b-4dd9-b5ee-da195cba83fe"),
    measurements_all=Input("ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac"),
    codeset_lookup=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    maps=Input("ri.foundry.main.dataset.92b496cb-d15b-4e3b-ad21-91341336cd27"),
    newunits=Input("ri.foundry.main.dataset.024ab317-edd4-4e0a-91a6-27b233c7ea3b"),
    minsmaxs=Input("ri.foundry.main.dataset.09b4a60a-3da4-4754-8a7e-0b874e2a6f2b"),
    invalidUnits=Input("ri.foundry.main.dataset.e91d2642-e865-4edb-a31d-00aa887b7c94"),
)
def my_compute_function(measurements_all, codeset_lookup, newunits, minsmaxs, maps, invalidUnits, ctx):
    ''' 
    ---- OVERALL OBJECTIVE ------------------------------------------------------------------------------------------------------------
    This script again adds the inferred units, as before, and takes the unit mappings file as a guide to perform unit conversions, 
    then at the end takes the values in harmonized_value_as_number and compares them to accepted ranges for each variable, to determine 
    whether the value should be nulled or not
    '''

    ctx.spark_session.conf.set("spark.sql.shuffle.partitions", 4000)
    # this file is huge, to repartition it: repartition returns a new RDD that has exactly numPartitions (300 in this case) partitions
    # that are distributed across the nodes of the computing cluster.
    measurements_all = measurements_all.repartition(4000, 'measurement_id').sortWithinPartitions('measurement_id').cache()

    # Just operate on certain columns of interest for harmonization - this also reduces compute memory required for the operations
    relevant_cols = ['measurement_id',
                     'measurement_concept_id',
                     'measurement_concept_name',
                     'data_partner_id',
                     'unit_concept_id',
                     'unit_concept_name',
                     'value_as_number']

    measurements = measurements_all.select(*relevant_cols)

    # Select only the codesets of interest i.e. variable that have been defined for harmonization

    relevant_codesets = minsmaxs.select('codeset_id')
    # Get all the concepts that correspond to the codesets
    codeset_lookup = codeset_lookup.select('codeset_id', 'concept_id').dropDuplicates() \
                                   .join(relevant_codesets.hint("broadcast"), 'codeset_id', 'inner')

    # ----------------------------------------------------------------------------------SELECT JUST THE ROWS THAT REQUIRE UNIT HARMONIZATION-----------

    # no operation needs to be performed on rows that don't have harmonization details, if you wanted to save unharmonized:
    # unharmonized = measurements.join(codeset_lookup.hint("broadcast"),(measurements.measurement_concept_id == codeset_lookup.concept_id),'leftanti') \
    #                            .drop(codeset_lookup.concept_id)

    # get just the rows that require harmonization
    # add the concepts and codesets from the codeset members table to the measurements table so we know which measurement goes with which variable
    df = measurements.join(codeset_lookup.hint("broadcast"), (measurements.measurement_concept_id == codeset_lookup.concept_id), 'inner') \
                     .drop(codeset_lookup.concept_id)

    df = df.withColumn('orig_unit_concept_id', col('unit_concept_id')) \
           .withColumn('orig_unit_concept_name', col('unit_concept_name'))  # save original column

    # -----------------------------------------------------------------------------------------------------------------ADD ON INFERRED UNITS-----------

    newunits = newunits.withColumnRenamed('codeset_id', 'codesetID') \
                       .withColumnRenamed('data_partner_id', 'data_partnerID') \
                       .withColumnRenamed('measurement_concept_name', 'measurement_concept_NAME') \
                       .withColumnRenamed('unit_concept_name', 'unit_concept_NAME')

    # add the inferred units info to measurements (will be used for measurements that have null or no matching concept in the units column)
    df = df.join(newunits.hint("broadcast"), (df.codeset_id == newunits.codesetID) &
                                             (df.data_partner_id == newunits.data_partnerID) &
                                             (df["measurement_concept_name"].eqNullSafe(newunits["measurement_concept_NAME"])) &
                                             (df["unit_concept_name"].eqNullSafe(newunits["unit_concept_NAME"])), 'left') \
        .drop('original_measurement_concept_id',
              'codesetID',
              'data_partnerID',
              'measurement_concept_NAME',
              'unit_concept_NAME')

    # remove any value from the original units that corresponds to an invalid unit, null or nmc (the idea of this is to make anything
    # without a unit defined into null, so that way when we apply coalesce later on, the null will be replaced by an
    # inferred unit from unitID and unitName)

    # make sure that the rows containing invalid units are flagged
    invalidUnits = invalidUnits.withColumnRenamed('omop_unit_concept_name', 'flag_omop_unit_concept_name') \
                               .select('codeset_id', 'flag_omop_unit_concept_name')
    df = df.join(invalidUnits, ((df.codeset_id == invalidUnits.codeset_id) &
                                (df.unit_concept_name == invalidUnits.flag_omop_unit_concept_name)), 'left')

    noUnits = [45947896,
               3040314,
               46237210,
               21498861,
               45878142,
               1032802,
               3245354,
               0,
               44814650,
               44814649,
               903640,
               1332722,
               903143,
               9177]

    df = df.withColumn("unit_concept_id", when(col("unit_concept_id").isin(noUnits), None)
                       .otherwise(when(col("flag_omop_unit_concept_name").isNotNull(), None)
                                  .otherwise(col("unit_concept_id")))) \
           .withColumn("unit_concept_name", when(col('unit_concept_id').isNull(), None)
                       .otherwise(when(col("flag_omop_unit_concept_name").isNotNull(), None)
                                  .otherwise(col('unit_concept_name')))) \
        .drop('flag_omop_unit_concept_name')

    df = df.withColumnRenamed('unit_concept_id', 'temp_unit_concept_id') \
           .withColumnRenamed('unit_concept_name', 'temp_unit_concept_name')  # save original column

    # fill in the units column with either the actual or inferred unit, prefering the actual unit where present
    df = df.withColumn('unit_concept_id', F.coalesce(df.temp_unit_concept_id, df.inferred_unit_concept_id)) \
           .withColumn('unit_concept_name', F.coalesce(df.temp_unit_concept_name, df.inferred_unit_concept_name))

    df = df.drop('MeasurementVar',
                 'inferred_unit_concept_id',
                 'inferred_unit_concept_name',
                 'measurement_concept_name')

    # -------------------------------------------------------------------------------------------PERFORM UNIT HARMONIZATION-----------

    # add on the harmonized units and map_function to the measurement table
    df = df.join(
        maps.select("original_measurement_concept_id", "original_unit_concept_id",
                    "harmonized_unit_concept_id", "map_function")
        .hint("broadcast"),
        (df["measurement_concept_id"].eqNullSafe(maps["original_measurement_concept_id"])) &
        (df["unit_concept_id"].eqNullSafe(maps["original_unit_concept_id"]))) \
        .drop('original_measurement_concept_id', 'original_unit_concept_id')

    # every row of measurement that had a relevant variable for harmonization now has a harmonized unit,
    # and the map_function (incl 'not_assigned')

    # create a dictionary of conversion formulas for each map_function
    function_dict = conversionsDictionary.conversions

    # add on a new column harmonized_value_as_number and initialize it for every row with null (empty string)
    df = df.withColumn("harmonized_value_as_number", F.lit(""))

    # go through all the keys of the dictionary and if the map_function matches the column in measurement, add the
    # value of the dictionary key using lambda, else leave the value as it is (either null or previously evaluated
    # value from a different map_function)

    cases = F
    for function_name, mapping_function in function_dict.items():
        cases = cases.when(F.col("map_function") == function_name, mapping_function(F.col("value_as_number")))
    cases = cases.otherwise(F.col("harmonized_value_as_number"))

    df = df.withColumn("harmonized_value_as_number", cases)

    # cast to double type
    df = df.withColumn("harmonized_value_as_number", df["harmonized_value_as_number"].cast(DoubleType())) \
           .drop("map_function")

    # 1:1 mapping for identity variables e.g. BUN/Creatinine ratio and pH
    df = df.join(minsmaxs.hint("broadcast"), 'codeset_id', 'left')  # will introduce the flag of null for units

    df = df.withColumn('harmonized_value_as_number', F.when(col('omop_unit_concept_id').isNull(), col('value_as_number'))
                       .otherwise(col('harmonized_value_as_number')))

    # null out implausible values
    df = df.withColumn("harmonized_value_as_number",
                       when(col('max_acceptable_value').isNotNull() &
                            ((col('harmonized_value_as_number') > col('max_acceptable_value')) | (col('harmonized_value_as_number') < col('min_acceptable_value'))), None)
                       .otherwise(col('harmonized_value_as_number')))

    df = df.withColumn("harmonized_unit_concept_id",
                       when(col('harmonized_value_as_number').isNull(), None)
                       .otherwise(col('harmonized_unit_concept_id')))

    # drop non-omop columns
    df = df.drop('measured_variable',
                 'omop_unit_concept_id',
                 'omop_unit_concept_name',
                 'max_acceptable_value',
                 'min_acceptable_value',
                 'temp_unit_concept_id',
                 'temp_unit_concept_name',
                 'codeset_id')

    # rename the column with the inferred units with the original column
    df = df.withColumnRenamed('unit_concept_id', 'unit_concept_id_or_inferred_unit_concept_id') \
           .withColumnRenamed('unit_concept_name', 'unit_concept_name_or_inferred_unit_concept_name') \
           .withColumnRenamed('orig_unit_concept_id', 'unit_concept_id') \
           .withColumnRenamed('orig_unit_concept_name', 'unit_concept_name')

    # add back in the unharmonized rows and other original columns
    df = df.drop('measurement_concept_id',
                 'data_partner_id',
                 'value_as_number',
                 'unit_concept_id',
                 'unit_concept_name',
                 'unit_concept_name_or_inferred_unit_concept_name',
                 'units')
    df = measurements_all.join(df, 'measurement_id', 'left')

    # ----------------------------------------------------------------------------------ADD BACK IN ROWS THAT DIDN'T UNDERGO HARMONIZATION----------

    # fill in the remaining units in the unharmonized rows for the column containing ALL units (original and inferred)
    df = df.withColumn('unit_concept_id_or_inferred_unit_concept_id', F.coalesce(df.unit_concept_id_or_inferred_unit_concept_id, df.unit_concept_id)) \
           .withColumn("harmonized_unit_concept_id", df["harmonized_unit_concept_id"].cast(IntegerType())) \

    df = df.withColumn("unit_concept_id_or_inferred_unit_concept_id",
                       df["unit_concept_id_or_inferred_unit_concept_id"].cast(IntegerType()))
    # harmonized unit cols and values are now added, return df
    return df
