from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
import csv
import tempfile
import shutil
from pyspark.sql import Row
from pyspark.sql import types as T


@transform(
    processed=Output(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/metadata/n3c_vocab_map"
    ),
    my_input=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: ACT/Site 411/transform/00 - unzipped/unzipped_raw_data"),
)
def my_compute_function(ctx, my_input, processed):

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

                map_data = list(r)
                # Handle missing header, possible column name differences, and different order of local/omop columns
                maybe_header_row = map_data[0]
                has_header = ("local" in maybe_header_row[0].lower()) or ("omop" in maybe_header_row[0].lower())
                local_code_col_first = ":" in map_data[1][0]
                if has_header:
                    for i in range(len(map_data[0])):
                        map_data[0][i] = "Original column name: " + str(map_data[0][i])

                if local_code_col_first:
                    MyRow = Row("local_prefix", "omop_vocab")
                else:
                    MyRow = Row("omop_vocab", "local_prefix")

                expected_num_fields = 2

                for row in map_data:
                    if len(row) == expected_num_fields:
                        yield MyRow(*row)

    files_df = my_input.filesystem().files(regex="(?i).*N3C.*VOCAB.*MAP.*")
    processed_rdd = files_df.rdd.flatMap(process_file)

    if processed_rdd.isEmpty():
        schema = T.StructType([
            T.StructField("local_prefix", T.StringType(), True),
            T.StructField("omop_vocab", T.StringType(), True),
        ])
        processed_df = processed_rdd.toDF(schema)
    else:
        processed_df = processed_rdd.toDF()

    # Order columns
    processed_df = processed_df.select("local_prefix", "omop_vocab")
    processed.write_dataframe(processed_df)
