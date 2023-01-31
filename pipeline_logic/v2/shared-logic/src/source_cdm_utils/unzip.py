'''

The get_newest_payload() and extract_filenames() functions are not great and should be re-written someday.
- tschwab

'''

import os
import tempfile
import shutil
from zipfile import ZipFile
from pyspark.sql import functions as F, types as T

# Read and write 100 MB chunks
CHUNK_SIZE = 100 * 1024 * 1024


def unzipLatest(foundryZip, regex, foundryOutput):
    fs = foundryZip.filesystem()
    files_df = fs.files(regex=regex)
    newest_file = get_newest_payload(files_df)
    unzip(foundryZip, foundryOutput, newest_file)


def unzip(foundryInput, foundryOutput, filename):
    inputFS = foundryInput.filesystem()
    outputFS = foundryOutput.filesystem()

    # Create a temp file to pass to zip library, because it needs to be able to .seek()
    with tempfile.NamedTemporaryFile() as temp:
        # Copy contents of file from Foundry into temp file
        with inputFS.open(filename, 'rb') as newest:
            shutil.copyfileobj(newest, temp)
            temp.flush()

        # For each file in the zip, unzip and add it to output dataset
        zipObj = ZipFile(temp.name)
        for filename in zipObj.namelist():
            with outputFS.open(filename, 'wb') as out:
                input_file = zipObj.open(filename)
                data = input_file.read(CHUNK_SIZE)
                while data:
                    out.write(data)
                    data = input_file.read(CHUNK_SIZE)


def extract_filenames(ctx, zip_file, regex, payload_filename):
    # Get the paths and determine the newest one
    fs = zip_file.filesystem()
    files_df = fs.files(regex=regex)
    newest_file = get_newest_payload(files_df)
    files_df = files_df.withColumn("newest_payload", F.when(F.col("path") == newest_file, F.lit(True)).otherwise(F.lit(False)))

    # Get just the filename, not the path
    get_basename = F.udf(lambda x: os.path.basename(x), T.StringType())
    ctx.spark_session.udf.register("get_basename", get_basename)
    files_df = files_df.withColumn("payload", get_basename(F.col("path")))

    # Select the needed data and repartition to a single file
    result = files_df.select("payload", "newest_payload")
    result = result.coalesce(1)

    # Write the result
    payload_filename.write_dataframe(result)


# Given a filesystem dataframe containing payload zip files, return the most recent payload name
def get_newest_payload(files_df):
    # Look for 8 digits at the end of the filepath representing payload date
    files_df = files_df.withColumn("payload_date", F.regexp_extract(F.col("path"), "(?i)(\\d{8})(.*)(\\.zip)$", 1))

    # Handle either yyyyMMDD or MMDDyyyy format
    files_df = files_df.withColumn(
        "processed_date",
        F.when(F.regexp_extract(F.col("path"), "(202.)\\d{4}", 1) == F.lit(""), F.concat(F.col("payload_date").substr(5, 4), F.col("payload_date").substr(1,4)))\
        .otherwise(F.col("payload_date"))
    )

    # If site submitted multiple files on the same day (e.g. "payload_20201015.zip" and "payload_20201015_1.zip", extract the increment
    files_df = files_df.withColumn("same_date_increment", F.regexp_extract(F.col("path"), "(?i)(\\d{8})(.*)(\\.zip)$", 2))

    # Sort by processed payload date, then by increment, then by modified time and grab the most recent payload
    files_df = files_df.orderBy(["processed_date", "same_date_increment", "modified"], ascending=False)
    newest_file_path = files_df.head().path

    return newest_file_path
