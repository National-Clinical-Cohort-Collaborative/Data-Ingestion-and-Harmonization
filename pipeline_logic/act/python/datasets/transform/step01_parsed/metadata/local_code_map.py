from transforms.api import transform, Input, Output
from pyspark.sql import Row
import csv
import tempfile
import shutil
from pyspark.sql import types as T
from pyspark.sql import functions as F


@transform(
    processed=Output('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/metadata/act_local_code_map'),
    my_input=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/00 - unzipped/unzipped_raw_data"),
)
def my_compute_function(my_input, processed):

    def process_file(file_status):
        with tempfile.NamedTemporaryFile() as t:
            # Copy contents of file from Foundry into temp file
            with my_input.filesystem().open(file_status.path, 'rb') as f_bytes:
                shutil.copyfileobj(f_bytes, t)
                t.flush()

            # Read the csv, line by line, and use csv.Sniffer to infer the delimiter
            with open(t.name, newline="") as f:
                r = csv.reader(f, delimiter="|")

                # Construct a pyspark.Row from our header row
                header = next(r)
                MyRow = Row(*header)
                expected_num_fields = len(header)

                for row in r:
                    if len(row) == expected_num_fields:
                        yield MyRow(*row)

    files_df = my_input.filesystem().files(regex="(?i).*ACT_STANDARD2LOCAL_CODE_MAP.*")
    processed_rdd = files_df.rdd.flatMap(process_file)

    if processed_rdd.isEmpty():
        schema = T.StructType([
            T.StructField("act_standard_code", T.StringType(), True),
            T.StructField("local_concept_cd", T.StringType(), True),
            T.StructField("name_char", T.StringType(), True),
            T.StructField("parent_concept_path", T.StringType(), True),
            T.StructField("concept_path", T.StringType(), True),
            T.StructField("path_element", T.StringType(), True),
        ])
        processed_df = processed_rdd.toDF(schema)
    else:
        processed_df = processed_rdd.toDF()

        # Convert empty strings to null values
        def blank_as_null(x):
            return F.when(F.col(x) != "", F.col(x)).otherwise(None)
        exprs = [blank_as_null(x).alias(x) for x in processed_df.columns]
        processed_df = processed_df.select(*exprs)

    processed.write_dataframe(processed_df)
