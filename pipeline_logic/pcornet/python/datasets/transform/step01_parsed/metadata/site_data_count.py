from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from pyspark.sql import Row
import csv
import tempfile
import shutil
from pyspark.sql import types as T


@transform(
    processed=Output(
        '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/metadata/data_counts_parsed',
        checks=Check(E.count().gt(0), 'Valid DATA_COUNTS file provided by site', on_error='WARN')
    ),
    my_input=Input('/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/00 - unzipped/unzipped_raw_data'),
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
                try:
                    dialect = csv.Sniffer().sniff(f.read(1024))
                    f.seek(0)
                    r = csv.reader(f, delimiter=dialect.delimiter, escapechar="\\")
                except Exception as e:
                    r = csv.reader(f, delimiter="|", escapechar="\\")

                # Construct a pyspark.Row from our header row
                header = next(r)
                MyRow = Row(*header)
                expected_num_fields = len(header)

                for row in r:
                    if len(row) == expected_num_fields:
                        yield MyRow(*row)

    files_df = my_input.filesystem().files(regex="(?i).*DATA_COUNTS.*")
    processed_rdd = files_df.rdd.flatMap(process_file)

    if processed_rdd.isEmpty():
        schema = T.StructType([
            T.StructField("TABLE_NAME", T.StringType(), True),
            T.StructField("ROW_COUNT", T.StringType(), True),
        ])
        processed_df = processed_rdd.toDF(schema)
    else:
        processed_df = processed_rdd.toDF()

    processed.write_dataframe(processed_df)
