from transforms.api import transform, Input, Output, Check
from transforms import expectations as E
from pyspark.sql import Row
import csv
import tempfile
import shutil
from datetime import datetime
from pyspark.sql import functions as F
from pcornet import omop_schemas


@transform(
    processed=Output(
        "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/metadata/manifest",
        checks=Check(E.negate(E.col("SITE_NAME").equals("[No manifest provided]")), 'Valid manifest provided by site', on_error='WARN')
    ),
    my_input=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/transform/00 - unzipped/unzipped_raw_data"),
    site_id_df=Input('/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 793'),
    data_partner_ids=Input("/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - ALL"),
    omop_vocab=Input("/UNITE/OMOP Vocabularies/vocabulary"),
    control_map=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PCORnet/Site 793/metadata/control_map")
)
def my_compute_function(ctx, my_input, site_id_df, data_partner_ids, omop_vocab, control_map, processed):

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
                header = [x.strip("\r") for x in header]
                MyRow = Row(*header)

                for row in r:
                    yield MyRow(*row)

    files_df = my_input.filesystem().files(regex="(?i).*MANIFEST.*")
    processed_rdd = files_df.rdd.flatMap(process_file)

    # Handle empty manifest
    if processed_rdd.isEmpty():
        schema = omop_schemas.manifest_schema
        data = [["[No manifest provided]" for _ in schema]]
        processed_df = ctx.spark_session.createDataFrame(data, schema)
        # Add CDM
        site_id_df = site_id_df.dataframe()
        data_partner_ids = data_partner_ids.dataframe()
        site_id_df = site_id_df.join(data_partner_ids, "data_partner_id", "left")
        try:
            cdm = site_id_df.head(1)[0].source_cdm
            processed_df = processed_df.withColumn("CDM_NAME", F.lit(cdm))
        except IndexError:
            pass
    else:
        processed_df = processed_rdd.toDF()
        curr_date = datetime.date(datetime.now())
        processed_df = processed_df.withColumn("CONTRIBUTION_DATE", F.lit(curr_date).cast("date"))

    omop_vocab = omop_vocab.dataframe()
    omop_vocab = omop_vocab.where(omop_vocab["vocabulary_id"] == "None").where(omop_vocab["vocabulary_name"] == "OMOP Standardized Vocabularies")
    vocabulary_version = omop_vocab.head(1)[0].vocabulary_version
    processed_df = processed_df.withColumn("N3C_VOCAB_VERSION", F.lit(vocabulary_version).cast("string"))

    # Compute approximate expected person count using the CONTROL_MAP file
    # Approximate xpected person count = (# rows in CONTROL_MAP) / 2 * 3
    control_map_total_count = control_map.dataframe().count()
    approx_expected_person_count = int((control_map_total_count / 2) * 3)
    processed_df = processed_df.withColumn("APPROX_EXPECTED_PERSON_COUNT", F.lit(approx_expected_person_count))

    processed.write_dataframe(processed_df)
