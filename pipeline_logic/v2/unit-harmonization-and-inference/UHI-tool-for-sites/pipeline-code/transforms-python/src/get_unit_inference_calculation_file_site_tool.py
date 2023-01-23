from transforms.api import configure, transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, ArrayType
from scipy import stats
import numpy as np
import pandas as pd
from unitConversions import conversionsDictionary
import pyspark
from scipy.stats import distributions


@configure(profile=['NUM_EXECUTORS_64', 'EXECUTOR_MEMORY_MEDIUM'])
@transform_df(
    Output("/UNITE/LDS/Measurement unit harmonization and inference tool for sites/inferred_units_calculation_site_tool"),
    codesetsLookup=Input("ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    measurements=Input("/UNITE/LDS/clean/measurement"),
    variables=Input("/UNITE/Unit Harmonization/canonical_units_of_measure"),
    invalidUnits=Input("/UNITE/Unit Harmonization/invalid_units_of_measure"),
    referenceValues=Input("/UNITE/LDS/Measurement unit harmonization and inference tool for sites/datasets/filtered_percentiles_table")

)
def my_compute_function(variables, invalidUnits, measurements, codesetsLookup, referenceValues, ctx):

    '''
    ---- OVERALL OBJECTIVE -------------------------------------------------------------------------------------------
    PERFORM K-S TESTS ON VALUES FOR A SITE'S LAB TEST THAT ARE MISSING UNITS TO DETERMINE THE CORRECT UNIT THAT SHOULD 
    BE ASSIGNED.

    '''
    ctx.spark_session.conf.set("spark.sql.shuffle.partitions", 3000)

    refLabs = referenceValues.select('codeset_id').distinct()

    concepts = variables.join(codesetsLookup, 'codeset_id', 'inner') \
                        .join(refLabs, 'codeset_id', 'inner')
    measurements_with_codesets = measurements.join(F.broadcast(concepts), measurements.measurement_concept_id == concepts.concept_id, 'inner')

    df = measurements_with_codesets

    df = df \
        .withColumn('units', F.concat(F.col('unit_concept_name'), F.lit('~'), F.col('unit_concept_id'))) \
        .withColumnRenamed('measured_variable', 'MeasurementVar')

    unit_data_by_measurement_all = df.select('MeasurementVar',
                                             'codeset_id',
                                             'units',
                                             'data_partner_id',
                                             'unit_concept_name',
                                             'omop_unit_concept_name',
                                             'measurement_concept_name',
                                             'value_as_number',
                                             'max_acceptable_value',
                                             'min_acceptable_value')

    # filter out rows where value_as_number is null
    measurementsDF = unit_data_by_measurement_all \
        .where(F.col("value_as_number").isNotNull()) \
        .where(~F.isnan(F.col("value_as_number"))) \
        .cache()

    # Obtain the map_function information that is needed to add to the test data to enable conversions
    refDF = measurementsDF.where(F.col("units").isNotNull()) \
    .where(measurementsDF.unit_concept_name != 'No matching concept') \
    .where(measurementsDF.unit_concept_name != 'No information') \
    .where(measurementsDF.unit_concept_name != 'Other')

    # remove any invalid units from the reference dataframe
    refDF = refDF.join(F.broadcast(invalidUnits), ((refDF.codeset_id == invalidUnits.codeset_id) &
                                      (refDF.unit_concept_name == invalidUnits.omop_unit_concept_name)), 'left_anti')

    # subset the reference values
    # Get a random sample of <=1000 rows per codeset_id
    # Ultimately we want sample size of <=100 rows, but need to account for the removal of implausible values after
    # the mapping function is applied. This avoids applying the mapping function to ALL rows only to choose 100 of them.
    w = W.partitionBy('codeset_id').orderBy(F.rand(42))
    refDF = refDF.select('*', F.rank().over(w).alias('rank')) \
                 .filter(F.col('rank') <= 1000) \
                 .drop('rank')

    refDF = refDF.withColumn('map_function', F.when(refDF.unit_concept_name == refDF.omop_unit_concept_name,
                                                    F.lit('x'))
                                              .otherwise(F.concat(F.col('unit_concept_name'),
                                                                  F.lit('_to_'),
                                                                  F.col('omop_unit_concept_name'),
                                                                  F.lit('_for_'),
                                                                  F.col('measurement_concept_name'))))


    # perform a conversion to the canonical unit for all of the data points with a non-null and non-nmc unit to use as
    # the reference distribution, then re-check the K-S test for each unitless site distributions (applying each
    # conversion factor to the datapoints within it until a good K-S score is found --> becomes the designated unit)

    # ----------------------------------------------------------------------------------CREATE TEST DATAFRAME-----------
    # testDF if unit_concept_name is null, NMC, NI, or Other (ie: measurements without units)
    testDF_1 = measurementsDF \
        .where((F.col("unit_concept_name").isNull()) |
               (measurementsDF.unit_concept_name == 'No matching concept') |
               (measurementsDF.unit_concept_name == 'No information') |
               (measurementsDF.unit_concept_name == 'Other'))

    testDF_2 = measurementsDF.join(invalidUnits, ((measurementsDF.codeset_id == invalidUnits.codeset_id) &
                                                  (measurementsDF.unit_concept_name == invalidUnits.omop_unit_concept_name)), 'leftsemi')

    # unit missing and invalid units rows to obtain the final test dataframe
    testDF = testDF_1.unionByName(testDF_2).drop('units')

    # Subset the test values
    # Get a random sample of <=100 rows per codeset_id+data_partner_id+measurement_concept_name
    w = W.partitionBy(['codeset_id', 'data_partner_id', 'measurement_concept_name']) \
         .orderBy(F.rand(42))
    testDF = testDF.select('*', F.rank().over(w).alias('rank')) \
                   .filter(F.col('rank') <= 100) \
                   .drop('rank')


    # --------------------------------------------------------------------------------PERFORM CONVERSIONS ON TEST DF----

    uniqueConversions = refDF.select('codeset_id', 'units', 'map_function') \
    .dropDuplicates(subset=['codeset_id', 'units'])

    # Get conversions to perform on testDF values
    testDF = testDF.join(uniqueConversions, 'codeset_id')

    function_dict = conversionsDictionary.conversions

    # go through all the keys of the dictionary and if the map_function matches the column in measurement,
    # add the value of the dictionary key using lambda, else leave the value as it is
    testDF = testDF.coalesce(64)
    convertedUnit = F.when(F.lit(False), F.lit(""))
    for (function_name, mapping_function) in function_dict.items():
        convertedUnit = convertedUnit.when(
            F.col('map_function') == function_name,
            mapping_function(F.col('value_as_number')))
    testDF = testDF.withColumn("convertedUnit", convertedUnit.otherwise(F.lit("")))

    # cast to double type
    testDF = testDF.withColumn("convertedUnit", testDF["convertedUnit"].cast(DoubleType()))

    # values by unit in testDF are now the test distributions for units that will be compared to the ref distribution

    # ---------------------------------------------------------------------------------PERFORM K-S TESTS--------------
    # check for converted unit
    @pandas_udf(ArrayType(DoubleType()))
    def get_ks_pval(refs, tests):


        def ks_1samp_ss(refs, x, args=(), alternative='two-sided', method='auto'):
            """
            Performs the one-sample Kolmogorov-Smirnov test for goodness of fit.
            This test compares the underlying distribution F(x) of a sample
            against a given continuous distribution G(x).
            """
            mode = method

            alternative = {'t': 'two-sided', 'g': 'greater', 'l': 'less'}.get(
                alternative.lower()[0], alternative)
            if alternative not in ['two-sided', 'greater', 'less']:
                raise ValueError("Unexpected alternative %s" % alternative)
            if np.ma.is_masked(x):
                x = x.compressed()

            N = len(x)
            x = np.sort(x)

            # calculate the cdf
            cdfvals = []
            for val in x:
                PrXx = len([item for item in refs if item <= val]) / len(refs)
                cdfvals.append(PrXx)

            def _compute_dplus(cdfvals, x):
                """Computes D+ as used in the Kolmogorov-Smirnov test.
                Parameters
                ----------
                cdfvals : array_like
                    Sorted array of CDF values between 0 and 1
                x: array_like
                    Sorted array of the stochastic variable itself
                Returns
                -------
                res: Pair with the following elements:
                    - The maximum distance of the CDF values below Uniform(0, 1).
                    - The location at which the maximum is reached.
                """
                n = len(cdfvals)
                dplus = (np.arange(1.0, n + 1) / n - cdfvals)
                amax = dplus.argmax()
                loc_max = x[amax]
                return (dplus[amax], loc_max)


            def _compute_dminus(cdfvals, x):
                """Computes D- as used in the Kolmogorov-Smirnov test.
                Parameters
                ----------
                cdfvals : array_like
                    Sorted array of CDF values between 0 and 1
                x: array_like
                    Sorted array of the stochastic variable itself
                Returns
                -------
                res: Pair with the following elements:
                    - Maximum distance of the CDF values above Uniform(0, 1)
                    - The location at which the maximum is reached.
                """
                n = len(cdfvals)
                dminus = (cdfvals - np.arange(0.0, n)/n)
                amax = dminus.argmax()
                loc_max = x[amax]
                return (dminus[amax], loc_max)

            # alternative == 'two-sided':
            Dplus, dplus_location = _compute_dplus(cdfvals, x)
            Dminus, dminus_location = _compute_dminus(cdfvals, x)
            if Dplus > Dminus:
                D = Dplus
                d_location = dplus_location
                d_sign = 1
            else:
                D = Dminus
                d_location = dminus_location
                d_sign = -1

            if mode == 'auto':  # Always select exact
                mode = 'exact'
            if mode == 'exact':
                prob = distributions.kstwo.sf(D, N)
            elif mode == 'asymp':
                prob = distributions.kstwobign.sf(D * np.sqrt(N))
            else:
                # mode == 'approx'
                prob = 2 * distributions.ksone.sf(D, N)
            prob = np.clip(prob, 0, 1)
            return [D, prob]

        # treat the CDF reference input per lab as if it were the population and use the 1samp KS test to obtain p-values for each set of test vals
        pvalres = [ks_1samp_ss(refs[n], tests[n]) if ((len(refs[n]) > 20) and (len(tests[n]) > 20)) else [0.0, 0.0] for n in range(len(refs))]
        return pd.Series(pvalres)

    refDFGrouped = referenceValues.select('codeset_id','CDFinfo').withColumnRenamed('CDFinfo','convertedUnitListRefs')

    testDF = testDF.select('MeasurementVar',
                           'codeset_id',
                           'units',
                           'data_partner_id',
                           'measurement_concept_name',
                           'unit_concept_name',
                           'convertedUnit')

    testDF = testDF.join(refDFGrouped, 'codeset_id', 'left')
    testDF = testDF.groupBy('MeasurementVar',
                            'codeset_id',
                            'units',
                            'data_partner_id',
                            'measurement_concept_name',
                            'unit_concept_name') \
                   .agg(F.first('convertedUnitListRefs').alias('refVals'), F.collect_list('convertedUnit').alias('testVals'))

    # Applying pandas udf to the two columns to obtain p-value for each row
    testDF = testDF.withColumn("KSpval", get_ks_pval(testDF.refVals, testDF.testVals))

    ks_tests_per_site_null_unit = testDF.select('MeasurementVar',
                                                'codeset_id',
                                                'data_partner_id',
                                                'measurement_concept_name',
                                                'unit_concept_name',
                                                'units',
                                                'refVals',
                                                'testVals',
                                                testDF.KSpval[1].alias('KSpvalue'))

    return ks_tests_per_site_null_unit.coalesce(1)

