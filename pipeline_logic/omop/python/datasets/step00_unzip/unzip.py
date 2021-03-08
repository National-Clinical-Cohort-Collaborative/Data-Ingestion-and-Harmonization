from transforms.api import transform, Input, Output
from myproject.utils import get_newest_payload
from pyspark.sql import functions as F
import tempfile
import shutil
import zipfile


@transform(
    processed=Output("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: OMOP/Site 25/transform/00 - unzipped/unzipped_raw_data"),
    zip_file=Input("/UNITE/Data Ingestion & OMOP Mapping/raw_data/Zipped Datasets/site_25_omop_raw_zips"),
)
def unzip(ctx, processed, zip_file):
    fs = zip_file.filesystem()
    files_df = fs.files(regex="(?i).*incoming.*\.zip")
    newest_file = get_newest_payload(files_df)

    # Create a temp file to pass to zip library
    with tempfile.NamedTemporaryFile() as t:
        # Copy contents of file from Foundry into temp file
        with fs.open(newest_file, 'rb') as f:
            shutil.copyfileobj(f, t)
            t.flush()

        z = zipfile.ZipFile(t.name)
        # For each file in the zip, unzip and add it to output dataset
        for filename in z.namelist():
            with processed.filesystem().open(filename, 'wb') as out:
                input_file = z.open(filename)
                CHUNK_SIZE = 100 * 1024 * 1024  # Read and write 100 MB chunks
                data = input_file.read(CHUNK_SIZE)
                while data:
                    out.write(data)
                    data = input_file.read(CHUNK_SIZE)