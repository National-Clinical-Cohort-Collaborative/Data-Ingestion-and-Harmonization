from transforms.api import transform, Input, Output


@transform(
    processed=Output("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/care_site"),
    my_input=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/02 - clean/care_site"),
)
def compute_function(my_input, processed):
    processed_df = my_input.dataframe()
    processed.write_dataframe(processed_df)
