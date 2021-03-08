from transforms.api import transform, Input, Output
from myproject.utils import perform_standard_mapping

domain = "dose_era"
concept_col = "drug_concept_id"


@transform(
    processed=Output("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/03 - prepared/dose_era"),
    my_input=Input("/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 605/transform/02 - clean/dose_era"),
    concept=Input("/UNITE/OMOP Vocabularies/concept"),
    concept_relationship=Input("/UNITE/OMOP Vocabularies/concept_relationship"),
)
def compute_function(my_input, concept, concept_relationship, processed):
    processed_df, concept_df, concept_relationship_df = my_input.dataframe(), concept.dataframe(), concept_relationship.dataframe()

    processed_df = perform_standard_mapping(processed_df, concept_df, concept_relationship_df, concept_col)

    processed.write_dataframe(processed_df)
