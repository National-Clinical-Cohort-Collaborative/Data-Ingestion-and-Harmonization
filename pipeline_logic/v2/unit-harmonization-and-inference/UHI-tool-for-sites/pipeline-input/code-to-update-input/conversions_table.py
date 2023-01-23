from transforms.api import transform_df, Input, Output
from pyspark.sql import Row
from pyspark.sql import functions as F
from dill.source import getsource
from pyspark.sql.functions import col, lower
from unitConversions import conversionsDictionary

@transform_df(
    Output("/UNITE/Unit Harmonization/conversions_table"),
    canonical_units=Input("/UNITE/Unit Harmonization/canonical_units_of_measure"),
    codeset_lookup=Input("/N3C Export Area/Concept Set Ontology/Concept Set Ontology/hubble_base/concept_set_members"),
)

def my_compute_function(canonical_units, codeset_lookup, ctx):

    conversions_dict = conversionsDictionary.conversions
    print(conversions_dict)

    #rows = [Row(key = x) for x in conversions_dict]
    rows = [Row(key = getsource(conversions_dict[x])) for x in conversions_dict]
    #return ctx.spark_session.createDataFrame(rows)

    df = ctx.spark_session.createDataFrame(rows)

    split_col = F.split(df['key'], ':')
    df = df.withColumn('conversion', split_col.getItem(0))
    df = df.withColumn('conversion_formula', split_col.getItem(2))
    df = df.withColumn('conversion_formula', F.regexp_replace('conversion_formula', '_col', ''))
    df = df.select('conversion','conversion_formula')
    df = df.withColumn('conversion_formula',F.regexp_replace('conversion_formula', "[^a-zA-Z0-9_/*+\-().]", ""))

    split_col2 = F.split(df['conversion'], '_to_')
    df = df.withColumn('unit', split_col2.getItem(0))
    df = df.withColumn('temp_col', split_col2.getItem(1))
    df = df.withColumn('unit',F.regexp_replace('unit', "  ", ""))
    df = df.withColumn('unit',F.regexp_replace('unit', "\"", ""))

    split_col3 = F.split(df['temp_col'], '_for_')
    df = df.withColumn('harmonized_unit', split_col3.getItem(0))
    df = df.withColumn('measurement_concept_name', split_col3.getItem(1))
    df = df.withColumn('measurement_concept_name',F.regexp_replace('measurement_concept_name', "\"", ""))

    df = df.select('unit','harmonized_unit','measurement_concept_name','conversion_formula')

    relevant_codesets = canonical_units.select('measured_variable','codeset_id')

    # Get all the concepts that correspond to the codesets
    codeset_lookup = codeset_lookup.select('concept_set_name','codeset_id','concept_name').dropDuplicates()
    codeset_lookup = codeset_lookup.join(relevant_codesets,'codeset_id','inner')
    df = df.join(codeset_lookup,(df.measurement_concept_name==codeset_lookup.concept_name),'left').drop('concept_set_name','concept_name')

    df = df.select([df.columns[-1]] + [df.columns[-2]] + df.columns[:-2])

    df = df.filter(F.col('measured_variable').isNotNull()) # remove conversions from old versions of codesets

    ## df = df.sort('measured_variable', 'unit', 'measurement_concept_name')
    df = df.orderBy(lower(col("measured_variable")))

    return df