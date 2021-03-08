from transforms.api import transform, Input, Output
from trinetx.utils import get_newest_payload
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os


@transform(
    payload_filename=Output("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/transform/00 - unzipped/payload_filename"),
    zip_file=Input("/UNITE/Data Ingestion & OMOP Mapping/raw_data/Zipped Datasets/site_77_trinetx_raw_zips"),
)
def unzip(ctx, payload_filename, zip_file):
    fs = zip_file.filesystem()
    files_df = fs.files(regex="(?i).*incoming.*\.zip")
    newest_file = get_newest_payload(files_df)
    files_df = files_df.withColumn("newest_payload", F.when(F.col("path") == newest_file, F.lit(True)).otherwise(F.lit(False)))

    get_basename = F.udf(lambda x: os.path.basename(x), T.StringType())
    ctx.spark_session.udf.register("get_basename", get_basename)
    files_df = files_df.withColumn("payload", get_basename(F.col("path")))

    payload_filename.write_dataframe(files_df.select("payload", "newest_payload"))
