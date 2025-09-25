from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from cms import utils, configs


def make_transform(dataset, groupName, group):
    @transform_df(
        Output(configs.transform + "03 - melted/initial/" + dataset.name + "_" + groupName),
        input_df=Input(configs.transform + "02 - schema applied/" + dataset.name),
        ahrq_codes=Input("/UNITE/LDS/AHRQ Safety Measure/ahrq_code_xwalk")
    )
    def compute_function(input_df, ahrq_codes):
        if "column_group" in group:
            span = [i for i in range(1, group["column_group"]["count"]+1)]
            columnNames = [group["column_group"]["column"] + str(i).zfill(2) for i in span]

            indexColumns = [F.lit(i).cast("string").alias("index") for i in span]
            keyColumns = [F.lit(col).alias("key") for col in columnNames]
            valueColumns = [F.col(col).alias("value") for col in columnNames]

            if dataset.dates["type"] == "static":
                startDateKeyColumns = [F.lit(dataset.dates["start"]).cast("string").alias("start_date_key") for _ in span]
                startDateValueColumns = [F.col(dataset.dates["start"]).cast("string").alias("start_date_value") for _ in span]
                endDateKeyColumns = [F.lit(dataset.dates["end"]).cast("string").alias("end_date_key") for _ in span]
                endDateValueColumns = [F.col(dataset.dates["end"]).cast("string").alias("end_date_value") for _ in span]
            elif dataset.dates["type"] == "dynamic":
                startDateKeyColumns = [F.lit(dataset.dates["start"] + str(i).zfill(2)).cast("string").alias("start_date_key") for i in span]
                startDateValueColumns = [F.col(dataset.dates["start"] + str(i).zfill(2)).cast("string").alias("start_date_value") for i in span]
                endDateKeyColumns = [F.lit(None).cast("string").alias("end_date_key") for _ in span]
                endDateValueColumns = [F.lit(None).cast("string").alias("end_date_value") for _ in span]

            groupZipped = zip(
                indexColumns, keyColumns, valueColumns,
                startDateKeyColumns, startDateValueColumns, endDateKeyColumns, endDateValueColumns
            )
        else:
            groupZipped = []

        if "column_list" in group:
            indexColumns = [F.lit(None).cast("string").alias("index") for _ in group["column_list"]]
            keyColumns = [F.lit(col).alias("key") for col in group["column_list"]]
            valueColumns = [F.col(col).alias("value") for col in group["column_list"]]

            if dataset.dates["type"] == "static":
                startDateKeyColumns = [F.lit(dataset.dates["start"]).cast("string").alias("start_date_key") for _ in group["column_list"]]
                startDateValueColumns = [F.col(dataset.dates["start"]).cast("string").alias("start_date_value") for _ in group["column_list"]]
                endDateKeyColumns = [F.lit(dataset.dates["end"]).cast("string").alias("end_date_key") for _ in group["column_list"]]
                endDateValueColumns = [F.col(dataset.dates["end"]).cast("string").alias("end_date_value") for _ in group["column_list"]]
            elif dataset.dates["type"] == "dynamic":
                startDateKeyColumns = [F.lit(None).cast("string").alias("start_date_key") for _ in group["column_list"]]
                startDateValueColumns = [F.lit(None).cast("string").alias("start_date_value") for _ in group["column_list"]]
                endDateKeyColumns = [F.lit(None).cast("string").alias("end_date_key") for _ in group["column_list"]]
                endDateValueColumns = [F.lit(None).cast("string").alias("end_date_value") for _ in group["column_list"]]

            listZipped = zip(
                indexColumns, keyColumns, valueColumns,
                startDateKeyColumns, startDateValueColumns, endDateKeyColumns, endDateValueColumns
            )
        else:
            listZipped = []

        zipped = list(listZipped) + list(groupZipped)
        columns = (
            F.struct(index, key, value, start_date_key, start_date_value, end_date_key, end_date_value)
            for index, key, value, start_date_key, start_date_value, end_date_key, end_date_value in zipped
        )

        melted_df = input_df.select(
            F.lit(dataset.name).alias("src_domain"),
            F.lit(groupName).alias("src_code_type"),
            F.lit(group["code"]).alias("src_vocab_code"),
            F.col("pkey").alias("pkey"),
            F.explode(F.array(*columns)).alias("__temp_column__")
        )

        melted_df = melted_df.select(
            "src_domain",
            "src_code_type",
            "src_vocab_code",
            "pkey",
            F.col("__temp_column__")["index"].alias("index"),
            F.col("__temp_column__")["key"].alias("src_column"),
            F.col("__temp_column__")["value"].alias("src_code"),
            F.col("__temp_column__")["start_date_key"].alias("start_date_key"),
            F.col("__temp_column__")["start_date_value"].alias("start_date_value"),
            F.col("__temp_column__")["end_date_key"].alias("end_date_key"),
            F.col("__temp_column__")["end_date_value"].alias("end_date_value"),
        )

        melted_df = melted_df.drop("__temp_column__")

        filtered_df = melted_df.filter(F.col("src_code") != "")

        if groupName == "dx":
            # remove AHRQ code values from the dx domains
            ahrq_codes = ahrq_codes.select("icd10_source_code").distinct()
            ahrq_codes = ahrq_codes.select(F.regexp_replace("icd10_source_code", "\\.", "").alias("src_code"))
            filtered_df = filtered_df.join(ahrq_codes, "src_code", "left_anti")
        return filtered_df

    return compute_function


def melt(dataset):
    transforms = (
        make_transform(dataset, groupName, groupSettings) for groupName, groupSettings in dataset.melting.items()
    )
    return transforms


transforms = utils.flat(melt(dataset) for dataset in configs.datasets)
