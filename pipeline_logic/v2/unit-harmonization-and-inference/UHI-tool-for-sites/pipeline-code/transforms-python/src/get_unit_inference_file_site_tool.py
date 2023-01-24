from transforms.api import configure, transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql import Window
from scipy import stats
import numpy as np
from pyspark.sql.functions import pandas_udf, greatest, least
from pyspark.sql.types import DoubleType
import pandas as pd
from unitConversions import conversionsDictionary

@configure(profile=['DRIVER_MEMORY_LARGE'])

@transform_df(
    Output("/UNITE/LDS/Measurement unit harmonization and inference tool for sites/inferred_units_site_tool"),
    inferred_unit_calc=Input("/UNITE/LDS/Measurement unit harmonization and inference tool for sites/inferred_units_calculation_site_tool"),
    canonicals_table=Input("/UNITE/Unit Harmonization/canonical_units_of_measure")

)
def my_compute_function(inferred_unit_calc, canonicals_table):
    '''
    ---- OVERALL OBJECTIVE -------------------------------------------------------------------------------------------
    FILTER UNITS FOR EACH MEASURED VARIABLE VALUE POPULATION (PARTNER SITE AND MEASUREMENT CONCDPT LEVEL) TO JUST THOSE 
    THAT FALL ABOVE THE KS P-VALUE THRESHOLD ('POTENTIAL INFERRED UNITS'). THEN OBTAIN A LIST OF MEASURED VARIABLES 
    THAT SHOULD NOT UNDERGO UNIT INFERRENCE DUE TO CONTAINING UNITS WHOSE VALUES DISPLAY PROPERTIES THAT INDICATE ANY 
    UNIT INFERERENCE WOULD BE UNRELIABLE.
    
    THIS MEASURE OF 'UNRELIABLE' IS ACHIEVED BY USING ONE MAIN THRESHOLD (THRESHOLD 1) AND THEN AN ADDITIONAL THESHOLD 
    (THRESHOLD 2), THE FORMER IS COMPOSED OF ONE CHECK (RATIO OF LOG DISPERSION / LOG MIN FOLD CHANGE OF ANY UNIT 
    CONVERSION FACTOR), AND THE LATTER IS COMPOSED OF THREE CHECKS (MEDIANS ARE TOO CLOSE TOGETHER, SPREAD OF THE VALUES 
    OVERALL IS TOO HIGH, AND MULTIPLE DISTINCT UNITS PRESENT).

    ANY ROWS OF INFERRED UNITS FROM MEASURED VARIABLES NAMED WITHIN EITHER OF TWO LISTS DERIVED FROM THE TWO THRESHOLDS 
    ABOVE IS THEN REMOVED.
    
    A FINAL CHECK IS THEN APPLIED, REMOVING ANY UNIT INFERENCE WHERE > 2 DISTINCT P-VALUES WERE PRESENT I.E. DIFFERENT UNITS 
    (CONVERSION FACTORS) ARE FOUND.

    THE FINAL RESULT ARE INFERRED UNITS THAT WE HAVE CONFIDENCE IN.
    '''

    df_canonical = canonicals_table
    df_inferred_unit_calc = inferred_unit_calc

    df_inferred_unit_calc = df_inferred_unit_calc \
        .filter(F.col("KSpvalue") >= 0.00002) \
        .withColumn("inferred_unit_concept_name", F.split(F.col("units"), "~").getItem(0)) \
        .withColumn("inferred_unit_concept_id", F.split(F.col("units"), "~").getItem(1))

    # Check medians and spread
    @pandas_udf(DoubleType())
    def array_median(x):
        medvals = [np.median(x[n].astype(np.float)) if ((len(x[n]) > 10)) else 0 for n in range(len(x))]
        return pd.Series(medvals)

    # Obtain the Median Absolute Deviation of the Median / median (relate measure of the spread of the datapoints)
    @pandas_udf(DoubleType())
    def array_MADM_over_median(x):
        MADM_over_median = [stats.median_absolute_deviation(n, axis=0, center=np.median, scale=1) /
                            np.median(n.astype(np.float))
                            for n in x]
        return pd.Series(MADM_over_median)

    w = Window.partitionBy('MeasurementVar', 'codeset_id')

    # Use collect_set and size functions to perform countDistinct over a window
    vardf = df_inferred_unit_calc \
        .withColumn('distinct_units', F.size(F.collect_set(F.col("units")).over(w))) \
        .withColumn("refVals_median", array_median("refVals")) \
        .withColumn("testVals_median", array_median("testVals"))

    '''
    ----- THRESHOLD CRITERIA 1 ---------------------------------------------------------------------------------------
    USING VALUE DISTRIBUTIONS (ALREADY CONVERTED TO THE CANONICAL UNIT) THAT FALL ABOVE THE KS TEST P-VALUE THRESHOLD,
    CHECK THE RATIO OF (LOG DISPERSION) / (LOG MINUMUM CONVERSION FACTOR FOLD CHANGE THAT NEEDS TO OCCUR FOR THE 
    MEASURED VARIABLE). IF THIS RATIO IS ABOVE 0.25, THE DISTINCT DISTRIBUTIONS OF POTENTIALLY DIFFERENT UNITS WILL BE 
    HIGHLY OVERLAPPING, THEREFORE NO UNIT CAN CONFIDENTLY BE ASSIGNED AND THE MEASURED VARIABLE SHOULD BE OMITTED FROM 
    UNIT INFERENCE
    '''

    df_conversions = vardf.select('MeasurementVar',
                                  'data_partner_id',
                                  'measurement_concept_name',
                                  'inferred_unit_concept_name',
                                  'testVals',
                                  'testVals_median')

    # Take the log10 of these median values
    df_thresholds1 = df_conversions.withColumn('log_testVals_median', F.log10(F.col('testVals_median')))

    # Get the variance of the log10 median values
    df_thresholds1 = df_thresholds1.groupby('MeasurementVar') \
                                   .agg(F.variance(F.col('log_testVals_median'))
                                        .alias('variance_of_log_vals_median_over_populations'))

    # Now add on the smallest conversion for each measured_variable
    df_canonical = df_canonical.withColumnRenamed('measured_variable', 'MeasurementVar') \
                               .select('MeasurementVar', 'omop_unit_concept_name').distinct()
    df_conversions = df_conversions.join(df_canonical, 'MeasurementVar', 'left') \
                                   .withColumn('value_as_number', F.lit(1))

    df_conversions = df_conversions \
        .withColumn('map_function',
                    F.when(df_conversions.inferred_unit_concept_name == df_conversions.omop_unit_concept_name,
                           F.lit('x'))
                    .otherwise(F.concat(F.col('inferred_unit_concept_name'),
                                        F.lit('_to_'),
                                        F.col('omop_unit_concept_name'),
                                        F.lit('_for_'),
                                        F.col('measurement_concept_name'))))

    function_dict = conversionsDictionary.conversions

    convertedUnit = F.when(F.lit(False), F.lit(""))
    for (function_name, mapping_function) in function_dict.items():
        convertedUnit = convertedUnit.when(
            F.col('map_function') == function_name,
            mapping_function(F.col('value_as_number')))
    df_conversions = df_conversions.withColumn("conversion_factor", convertedUnit.otherwise(F.lit("")).cast(DoubleType()))

    '''
    At this stage we have all the conversion factors  for every possible inferred unit that falls above the p-value
    threshold (later used for comparison to dispersion of values to evaluate whether measurement variable should be
    subject to unit inference omission)
    '''

    ## CAUSES OOM IF NOT DRIVER_LARGE PROFILE ########################################################################

    df_conversions = df_conversions.filter(F.col('conversion_factor').isNotNull()) \
                                   .filter(df_conversions.conversion_factor != 1)

    ##################################################################################################################

    df_conversions = df_conversions.withColumn('conversion_fold_change',
                                               F.when((F.col('conversion_factor') < 1), 1/F.col('conversion_factor'))
                                                .otherwise(F.col('conversion_factor')))

    df_conversions = df_conversions.groupby('MeasurementVar') \
                                   .agg(F.min('conversion_fold_change').alias('min_conversion_fold_change'))

    df_conversions = df_conversions.withColumn("min_conversion_fold_change",
                                               F.when(df_conversions["MeasurementVar"] == 'Temperature', 2.7)
                                                .otherwise(df_conversions["min_conversion_fold_change"]))

    df_joined = df_thresholds1.join(df_conversions, 'MeasurementVar', 'inner')

    # Take the sqrt to obtain the stdev
    df_joined = df_joined.withColumn('std_of_log_medians', F.sqrt(F.col('variance_of_log_vals_median_over_populations')))

    # Compare the ratio of  (std of log medians)  /  log of (min_conversion_fold_change)
    # if > 0.25, then add that measured variable to the list for omitting unit inference
    df_inference_omission = df_joined.withColumn('ratio_for_threshold_comparison',
                                                 (F.col('std_of_log_medians') / F.log10(F.col('min_conversion_fold_change')))) \
                                     .filter(F.col('ratio_for_threshold_comparison') > 0.25)

    # Whatever is in the 'measured_variable column at that point gets turned into a list for unit inference omission
    df_inference_omission_list = [x.MeasurementVar for x in df_inference_omission.select('MeasurementVar').distinct().collect()]  # noqa

    '''
    ----- THRESHOLD CRITERIA 2 ---------------------------------------------------------------------------------------
    PERFORM ADDITIONAL CHECKS TO DETERMINE MEASURED VARIABLES THAT SHOULD BE OMITTED FROM UNIT INFERENCE - THE 
    OBJECTIVE HERE IS SIMILAR TO THE OBJECTIVE FOR THRESHOLD 1 - BUT HERE WE USE CHECKS FOR SPREAD OF THE VALUES, 
    CLOSENESS OF MEDIANS ACROSS DISTRIBUTIONS, AND DISTINCT UNIT COUNTS, IN ORDER TO CREATE A SECOND LIST OF 
    MEASURED VARIABLES WHERE NO ONE UNIT CAN UNIQUELY BE ASSIGNED TO THE VALUE DISTRIBUTION
    '''

    df_thresholds2 = vardf.withColumn("foldchange",
                                      ((greatest(vardf.refVals_median, vardf.testVals_median))
                                       / (least(vardf.refVals_median, vardf.testVals_median)))) \
                          .withColumn("testVals_MADM_over_median", array_MADM_over_median("testVals"))

    grp_window = Window.partitionBy('codeset_id')
    grp_percentile = F.expr('percentile_approx(testVals_MADM_over_median, 0.5)')

    df_thresholds2 = df_thresholds2.withColumn('median_of_testVals_MADM_over_median', grp_percentile.over(grp_window))

    df_thresholds2 = df_thresholds2.groupBy('MeasurementVar',
                                            'codeset_id',
                                            'data_partner_id',
                                            'measurement_concept_name',
                                            'KSpvalue') \
                                   .agg(F.mean(df_thresholds2.foldchange)
                                         .alias('mean_foldchange'),
                                        F.first(df_thresholds2.median_of_testVals_MADM_over_median)
                                         .alias('median_of_testVals_MADM_over_median'),
                                        F.first(df_thresholds2.distinct_units)
                                         .alias('distinct_units'))

    df_thresholds2 = df_thresholds2 \
        .groupBy('MeasurementVar',
                 'codeset_id',
                 'data_partner_id',
                 'measurement_concept_name',
                 'KSpvalue') \
        .agg(F.sum(F.when(((df_thresholds2.mean_foldchange > 3) & (df_thresholds2.mean_foldchange < 15)), 1)
                    .otherwise(0))
              .alias('count_foldchange_3_15'),
             F.first(df_thresholds2.median_of_testVals_MADM_over_median)
              .alias('median_of_testVals_MADM_over_median'),
             F.first(df_thresholds2.distinct_units)
              .alias('distinct_units'))

    df_thresholds2 = df_thresholds2.groupBy('MeasurementVar',
                                            'codeset_id') \
                                   .agg(F.count(df_thresholds2.count_foldchange_3_15)
                                         .alias('count_foldchange_3_15'),
                                        F.sum(df_thresholds2.count_foldchange_3_15)
                                         .alias('sum_foldchange_3_15'),
                                        F.first(df_thresholds2.median_of_testVals_MADM_over_median)
                                         .alias('median_of_testVals_MADM_over_median'),
                                        F.first(df_thresholds2.distinct_units)
                                         .alias('distinct_units'))

    vardf_thresholds = df_thresholds2.withColumn('perc_close', ((df_thresholds2.sum_foldchange_3_15 / df_thresholds2.count_foldchange_3_15)*100))

    # Filter out the variables where inference can't be applied
    vardf_thresholds = vardf_thresholds.filter((vardf_thresholds.perc_close > 2) &
                                               (vardf_thresholds.distinct_units > 1) &
                                               (vardf_thresholds.median_of_testVals_MADM_over_median > 0.75))

    distinct_ids = [x.MeasurementVar for x in vardf_thresholds.select('MeasurementVar').distinct().collect()]  # noqa

    '''
    ----- REMOVE MEASURED VARIABLES ----------------------------------------------------------------------------------
    REMOVE MEASURED VARIABLES FROM EITHER OF THE ABOVE CRITERIA FROM THE DATAFRAME
    '''

    df_inferred_unit_calc = df_inferred_unit_calc \
        .filter(~F.col("MeasurementVar").isin(distinct_ids)) \
        .filter(~F.col("MeasurementVar").isin(df_inference_omission_list))

    # Check if there are greater than two p-values above 0.0001 that are different
    # (if they're the same it's OK as it'll be from the same unit)
    w = Window.partitionBy('MeasurementVar',
                           'codeset_id',
                           'data_partner_id',
                           'measurement_concept_name',
                           'unit_concept_name')

    # Use collect_set and size functions to perform countDistinct over a window
    df_inferred_unit_calc = df_inferred_unit_calc \
        .withColumn('distinct_ks_pvalue', F.size(F.collect_set("KSpvalue").over(w))) \
        .filter(F.col('distinct_ks_pvalue') < 2)

    df_inferred_unit_calc = df_inferred_unit_calc \
        .join(df_inferred_unit_calc.groupBy('MeasurementVar',
                                            'codeset_id',
                                            'data_partner_id',
                                            'measurement_concept_name',
                                            'unit_concept_name')
                                   .agg(F.max('KSpvalue').alias('KSpvalue')),
              on='KSpvalue', how='leftsemi')

    df_inferred_unit_calc = df_inferred_unit_calc \
        .groupBy('MeasurementVar',
                 'codeset_id',
                 'data_partner_id',
                 'measurement_concept_name',
                 'unit_concept_name') \
        .agg(F.first('units')
              .alias('units'),
             F.first('inferred_unit_concept_name')
              .alias('inferred_unit_concept_name'),
             F.first('inferred_unit_concept_id')
              .alias('inferred_unit_concept_id'))

    return df_inferred_unit_calc
