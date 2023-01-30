"""
This is a very precisely created file, do not change it. It was created to trick Foundry Templates into giving us the
path of the root folder of the deployed template. In generate-anchor.py, we use the anchor path defined in path.py to
create a dummy anchor dataset at the root of the project. Then when a new instance of the template is deployed, this
anchor path is automatically replaced with the path of the anchor dataset in the deployed template. Then to get the
root, we simply remove the name "anchor". Finally, we can use this root path in the rest of the repo. Doing this
allowed us to massively de-duplicate repeated code, in some steps reducing the number of lines of code by more than 90%.
"""

anchor = "/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 77/anchor"
root = anchor[:-len("anchor")]
transform = root + "transform/"
metadata = root + "metadata/"
union_staging = root + "union_staging/"

input_zip = "/UNITE/Data Ingestion & OMOP Mapping/raw_data/Zipped Datasets/site_77_trinetx_raw_zips"
site_id = '/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - Site 77'
all_ids = "/UNITE/Data Ingestion & OMOP Mapping/raw_data/data partner id tables/Data Partner IDs - ALL"
mapping = '/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/TriNetX Vocab Correction Mapping Table'
vocab = "/UNITE/OMOP Vocabularies/vocabulary"
concept = "/N3C Export Area/OMOP Vocabularies/concept"
