from transforms.api import configure
from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F
from pyspark.sql.functions import col, create_map, lit, when
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from itertools import chain


@configure(profile=['NUM_EXECUTORS_32', 'EXECUTOR_MEMORY_MEDIUM', 'DRIVER_MEMORY_LARGE'])
@transform_df(
    Output("/UNITE/LDS/harmonization/unit_mappings"),
    df=Input("/UNITE/Unit Harmonization/canonical_units_of_measure"),
    conceptsets=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    newunits=Input("/UNITE/LDS/harmonization/harmonization_pipeline/inferred_units_site_tool"),
    measurements=Input("/UNITE/LDS/clean/measurement"),
    invalidUnits= Input("/UNITE/Unit Harmonization/invalid_units_of_measure"),
)
def my_compute_function(df, conceptsets, measurements, invalidUnits, newunits):

    ''' 
    Produce a mapping table to use as a reference, for the formulae conversions process, and for debugging purposes
    two main purposes: (1) add inferred unit in for measurements (as those need conversion functions too for units to 
    generate harmonized unit) and (2) generate every possible from:to unit combination and add an associated 'map_function', 
    which defines in the next set of code which unit conversion formulae should be applied
    '''
    #### add all concept IDs to each codeset ID ####

    df = df.drop('max_acceptable_value', 'min_acceptable_value')

    # Add in the codesets and the inferred units based on each variable (defined by the codesets)

    conceptsets = conceptsets.select('codeset_id', 'concept_id').dropDuplicates()

    # add the inferred units information (to use for measurements that have null or no matching concept in the units column)
    measurements = measurements.join(F.broadcast(conceptsets), (measurements.measurement_concept_id == conceptsets.concept_id), 'left') \
                               .drop('concept_id')

    newunits = newunits.withColumnRenamed('codeset_id', 'codesetID') \
                       .withColumnRenamed('data_partner_id', 'data_partnerID') \
                       .withColumnRenamed('measurement_concept_name', 'measurement_concept_NAME') \
                       .withColumnRenamed('unit_concept_name', 'unit_concept_NAME')

    # -----------------------------------------------------------------------------------------------------------------------ADD INFERRED UNITS-----------

    # add the inferred units info to measurements (will be used for measurements that have null or no matching concept in the units column, or are invalid units)
    measurements = measurements.join(F.broadcast(newunits), (measurements.codeset_id == newunits.codesetID) &
                                                                 (measurements.data_partner_id == newunits.data_partnerID) & 
                                                                 (measurements["measurement_concept_name"].eqNullSafe(newunits["measurement_concept_NAME"])) & 
                                                                 (measurements["unit_concept_name"].eqNullSafe(newunits["unit_concept_NAME"])), 'left') \
                                             .drop('original_measurement_concept_id', 
                                                   'codesetID',
                                                   'data_partnerID',
                                                   'measurement_concept_NAME',
                                                   'unit_concept_NAME',
                                                   'MeasurementVar')
    
    # make sure that the rows containing invalid units are flagged
    invalidUnits = invalidUnits.withColumnRenamed('omop_unit_concept_name', 'flag_omop_unit_concept_name') \
                               .select('codeset_id', 'flag_omop_unit_concept_name')
    measurements = measurements.join(F.broadcast(invalidUnits), ((measurements.codeset_id == invalidUnits.codeset_id) &
                                    (measurements.unit_concept_name == invalidUnits.flag_omop_unit_concept_name)), 'left')

    # remove any value from the original units that corresponds to invalid units, null or nmc (the idea of this is to make anything without a unit defined into null, so 
    # that way when we apply coalesce later on, the null will be replaced by an inferred unit from unitID and unitName)

    noUnits = [45947896,3040314,46237210,21498861,45878142,1032802,3245354,0,44814650,44814649,903640,1332722,903143,9177]

    measurements = measurements.withColumn("unit_concept_id", when(col("unit_concept_id").isin(noUnits), None)
                                                .otherwise(when(col("flag_omop_unit_concept_name").isNotNull(), None)
                                                .otherwise(col("unit_concept_id")))) \
                                .withColumn("unit_concept_name", when(col('unit_concept_id').isNull(), None)
                                                .otherwise(when(col("flag_omop_unit_concept_name").isNotNull(), None)
                                                .otherwise(col('unit_concept_name')))) \
                                .drop('flag_omop_unit_concept_name')


    # Add the inferred units (coalesce looks for the first non-null value in a column and applies it to the specified column)

    measurements = measurements.withColumn('unit_concept_id', F.coalesce(measurements.unit_concept_id,measurements.inferred_unit_concept_id)) \
                               .withColumn('unit_concept_name', F.coalesce(measurements.unit_concept_name,measurements.inferred_unit_concept_name)) \
                               .drop('codeset_id',
                                     'inferred_unit_concept_id',
                                     'inferred_unit_concept_name')

    #-----------------------------------------------------------------------------------------------------------------------ADD ON CANONICAL UNIT INFO--

    # get just the codesets of interest from the task team canonical units table (these are the only ones we'll have harmonized units attached)
    # some variables in the canonical units table had null for canonical units, so drop these as they aren't useful
    df = df.filter(df.omop_unit_concept_id.isNotNull()).dropDuplicates()

    # get all the concepts for these codesets
    df = df.join(F.broadcast(conceptsets), 'codeset_id').dropDuplicates()

    # drop all the irrelevant columns (the surplus column measured_variable also incidentally create duplicates in concept:canonical unit rows)
    columns_to_drop = ['measured_variable', 'codeset_id', 'max_acceptable_value', 'min_acceptable_value']
    df = df.drop(*columns_to_drop).dropDuplicates()

    # now we have non-redundant mapping of concepts to canonical units

    #### add all unique measurement units for each concept ID ####
    
    df = df.withColumnRenamed("concept_id", "lookup_measurement_concept_id")

    # get the important concept and unit info from the measurement table
    columns_to_keep = ['measurement_concept_id', 'measurement_concept_name', 'unit_concept_id', 'unit_concept_name']
    measurements_small = measurements.select(*columns_to_keep).dropDuplicates()

    # join on all canonical units (where present) for the concepts present in measurements
    df = measurements_small.join(
        F.broadcast(df),on=measurements_small["measurement_concept_id"].eqNullSafe(df["lookup_measurement_concept_id"]),how='left')\
                                                          .drop("lookup_measurement_concept_id").dropDuplicates()

    #---------------------------------------------------------------------------------------------------------GENERATE THE MAP FUNCTION AS A NEW COLUMN--

    # add on the map_function name based on the units and concepts present
    df = df.withColumn('map_function', F.when(df.unit_concept_name == df.omop_unit_concept_name, F.lit('x'))
                                        .otherwise(F.concat(F.col('unit_concept_name'),
                                                            F.lit('_to_'),
                                                            F.col('omop_unit_concept_name'),
                                                            F.lit('_for_'),
                                                            F.col('measurement_concept_name'))))
    # where any unit or concept is null, the map_function will be null, so instead put 'not_assigned' for map_function
    df = df.fillna('not_assigned', subset=['map_function'])

    # rename the cols to desired name (harmonized) and prefix 'original_' to enable an easier join later
    df = df.withColumnRenamed('omop_unit_concept_name', 'harmonized_unit_concept_name') \
           .withColumnRenamed('omop_unit_concept_id', 'harmonized_unit_concept_id') \
           .withColumnRenamed('unit_concept_id', 'original_unit_concept_id')
    unit_mappings = df.withColumnRenamed('measurement_concept_id', 'original_measurement_concept_id')

    return unit_mappings
